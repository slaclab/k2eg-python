import re
import uuid
import queue
import msgpack
import logging
import threading
import datetime

from enum import Enum
from time import sleep
from readerwriterlock import rwlock
from confluent_kafka import KafkaError
from k2eg.broker import Broker, SnapshotProperties
from k2eg.serialization import MessagePackSerializable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any, Optional

logger = logging.getLogger(__name__) 
_protocol_regex = r"^(pva?|ca)://((?:[A-Za-z0-9-_:]+(?:\.[A-Za-z0-9-_]+)*))$"

def _filter_pv_uri(uri: str):
    match = re.match(_protocol_regex, uri)
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None

class OperationTimeout(Exception):
    """Exception raised when the timeout is expired on operation"""
    def __init__(self, message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class OperationError(Exception):
    """Exception raised when the timeout is expired on operation"""
    def __init__(self, error, message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.error = error

class SnapshotState(Enum):
    INITIALIZED = 0
    HEADER_RECEVED = 1
    DATA_ACQUIRING = 2
    TAIL_RECEIVED = 3

@dataclass
class RecurringIteration:
    """Track sequencing state for one recurring snapshot iteration."""
    iteration: int
    header_timestamp: Optional[int] = None
    tail_timestamp: Optional[int] = None
    tail_seq: Optional[int] = None
    total_messages: Optional[int] = None
    # msg_seq=1 is always the header, so data can only be appended once the
    # client has seen the next contiguous sequence number starting from 2.
    next_expected_seq: int = 2
    results: Dict[str, List[Any]] = field(default_factory=dict)
    # Out-of-order data messages are parked here by msg_seq until all lower
    # sequence numbers have been flushed into results.
    deferred_messages: Dict[int, tuple[str, Any]] = field(default_factory=dict)
    header_submission_seq: Optional[int] = None
    tail_submission_seq: Optional[int] = None
    submission_buffers: Dict[int, "SubmissionBuffer"] = field(default_factory=dict)
    invalid_reason: Optional[str] = None
    merge_queue: queue.SimpleQueue = field(default_factory=queue.SimpleQueue)

    def is_complete(self, use_submission_seq: bool = False) -> bool:
        """True when we have enough ordered data to publish the iteration."""
        if self.invalid_reason is not None:
            return False
        if self.tail_seq is None:
            return False
        if use_submission_seq:
            if self.total_messages is None:
                return False
            expected_data_messages = max(self.total_messages - 2, 0)
            received_data_messages = sum(
                len(buffer.messages) for buffer in self.submission_buffers.values()
            )
            return received_data_messages >= expected_data_messages
        return self.next_expected_seq > self.tail_seq - 1


@dataclass
class SubmissionBuffer:
    """Track buffered recurring snapshot data for one submission batch."""
    submission_seq: int
    messages: List[tuple[str, Any]] = field(default_factory=list)
    last_msg_seq: Optional[int] = None
    enqueued: bool = False

@dataclass
class Snapshot:
    handler: Callable[[str, Dict[str, Any]], None]
    properties: SnapshotProperties = None
    publishing_topic: str = None
    state: SnapshotState = SnapshotState.INITIALIZED
    timestamp: datetime.datetime = None
    interation: int = 0
    pv_list: List[str] = field(default_factory=list)
    results: Dict[str, List[Any]] = field(default_factory=dict[str, List[Any]])
    active_iteration: Optional[RecurringIteration] = None
    next_iteration: Optional[RecurringIteration] = None

    def init(self):
        # fill the results with empty lists for each pv
        for pv in self.pv_list:
            self.results[pv] = []

    def create_iteration(self, iteration: int, header_timestamp: Optional[int] = None) -> RecurringIteration:
        new_iteration = RecurringIteration(
            iteration=iteration,
            header_timestamp=header_timestamp,
            results={pv: [] for pv in self.pv_list},
        )
        return new_iteration

    def clear(self):
        """Clear all lists in the results dictionary without removing the keys."""
        for key in self.results:
            self.results[key] = []
        self.timestamp = None
        self.interation = 0
        self.state = SnapshotState.INITIALIZED
        self.active_iteration = None
        self.next_iteration = None
            
class dml:
    """K2EG client"""
    def __init__(
            self, 
            environment_id: str, 
            app_name: str,
            group_name: str = None,
            poll_timeout: float = 0.01):
        if app_name is None:
            raise ValueError(
                "The app name is mandatory"
            )
        self.__broker = None
        self.__thread = None
        self.__broker = Broker(environment_id, app_name, group_name)
        self.__lock = rwlock.RWLockFairD()
        self.__reply_partition_assigned = threading.Event()
        self.poll_timeout = poll_timeout
        #reset to listen form now
        self.__consume_data = True
        self.__thread = threading.Thread(
            target=self.__consumer_handler
        )
        self.__thread.start()
        self.__monitor_pv_handler = {}
        self.reply_wait_condition = threading.Condition()
        self.reply_ready_event = threading.Event()
        self.reply_message = {}
        # Track reply IDs that timed out so we can detect late Kafka replies.
        self.__timed_out_replies = {}
        self.__timed_out_replies_ttl_sec = 300
        #contain a vector for each reply id where snapshot are stored
        self.reply_snapsthot_message = {}
        self.reply_recurring_snapsthot_message = {}
        logger.info(
            f"Created dml instance for environment '{environment_id}' "+
            f"and application '{app_name}' with group '{group_name}' with poll timeout '{poll_timeout}'"
        )

    def __del__(self):
        # Perform cleanup operations when the instance is deleted
        self.close()

    def __from_json(self, j_msg):
        print('__from_json')

    def __from_msgpack(self, m_msg):
        msg_id = None
        converted_msg = None
        decodec_msg = msgpack.loads(m_msg)
        if not isinstance(decodec_msg, dict):
            return msg_id, converted_msg
        
        if 'reply_id' in decodec_msg:
            msg_id = decodec_msg['reply_id']
            converted_msg = decodec_msg
        elif 'snapshot_name' in decodec_msg:
            msg_id = decodec_msg['snapshot_name']
            converted_msg = decodec_msg
        else:
            msg_id = list(decodec_msg.keys())[0]
            converted_msg = decodec_msg
        
        # Add message-size key with the received msgpack size
        converted_msg['message-size'] = len(m_msg)
        return msg_id, converted_msg

    def __from_msgpack_compack(self, mc_msg):
        print('__from_msgpack_compack')

    def __decode_message(self, msg):
        """ Decode single message
        """
        msg_id = None
        converted_msg = None
        headers = msg.headers()
        if headers is None:
            logger.error("Message without header received")
            return msg_id, converted_msg
        
        for key, value in headers:
            if key == 'k2eg-ser-type':
                st = value.decode('utf-8').lower()
                if st == "json":
                    msg_id, converted_msg = self.__from_json(
                        msg.value()
                        )
                elif st == "msgpack":
                    msg_id, converted_msg = self.__from_msgpack(
                        msg.value()
                        )
                elif st == "msgpack-compact":
                    msg_id, converted_msg = self.__from_msgpack_compack(
                        msg.value()
                        )
                break   
        if msg_id is None:
            logger.debug(
                "Unable to decode incoming message: missing/unsupported 'k2eg-ser-type' header. headers=%s",
                headers,
            )
        return msg_id, converted_msg

    def process_event(self, topic_name, msg_id, decoded_message):
        logger.debug(f"received event on topic {topic_name}")
        self.__monitor_pv_handler[msg_id](msg_id, decoded_message)
    
    @staticmethod
    def _extract_remaining_dict_item(d: dict):
        """Extract the first remaining key-value pair from a dict."""
        return next(iter(d.items()))

    def __set_active_iteration(self, snapshot: Snapshot, iteration_state: RecurringIteration):
        # Mirror the active iteration into the legacy snapshot fields so the public
        # handler payload and existing logging keep the same shape.
        snapshot.active_iteration = iteration_state
        snapshot.interation = iteration_state.iteration
        snapshot.timestamp = iteration_state.header_timestamp
        snapshot.results = iteration_state.results
        snapshot.state = SnapshotState.HEADER_RECEVED

    @staticmethod
    def __uses_submission_seq(snapshot: Snapshot) -> bool:
        return bool(
            snapshot.properties is not None
            and snapshot.properties.sub_push_delay_msec > 0
        )

    def __discard_iteration(self, snapshot: Snapshot, iteration_state: RecurringIteration):
        if iteration_state is snapshot.active_iteration:
            self.__promote_next_iteration(snapshot)
        elif iteration_state is snapshot.next_iteration:
            snapshot.next_iteration = None

    def __invalidate_iteration(self, snapshot: Snapshot, iteration_state: RecurringIteration, from_topic: str, reason: str):
        if iteration_state.invalid_reason is not None:
            return
        iteration_state.invalid_reason = reason
        logger.error(
            "Discarding recurring snapshot %s iteration %s: %s",
            from_topic,
            iteration_state.iteration,
            reason,
        )
        self.__discard_iteration(snapshot, iteration_state)

    def __promote_next_iteration(self, snapshot: Snapshot):
        if snapshot.next_iteration is None:
            snapshot.clear()
            return

        # The next iteration may already have buffered data because Kafka delivery
        # can overlap consecutive iterations. Promote it immediately and drain any
        # contiguous seq numbers we already have.
        next_iteration = snapshot.next_iteration
        snapshot.next_iteration = None
        self.__set_active_iteration(snapshot, next_iteration)
        if self.__uses_submission_seq(snapshot):
            self.__refresh_iteration_legacy_fields(snapshot, next_iteration)
        elif next_iteration.deferred_messages:
            self.__flush_iteration_messages(snapshot, next_iteration)
        self.__update_snapshot_state(snapshot)

    def __update_snapshot_state(self, snapshot: Snapshot):
        iteration_state = snapshot.active_iteration
        if iteration_state is None:
            snapshot.state = SnapshotState.INITIALIZED
            return
        if iteration_state.tail_seq is not None:
            snapshot.state = SnapshotState.TAIL_RECEIVED
        elif self.__uses_submission_seq(snapshot) and iteration_state.submission_buffers:
            snapshot.state = SnapshotState.DATA_ACQUIRING
        elif iteration_state.next_expected_seq > 2:
            snapshot.state = SnapshotState.DATA_ACQUIRING
        else:
            snapshot.state = SnapshotState.HEADER_RECEVED

    def __flush_iteration_messages(self, snapshot: Snapshot, iteration_state: RecurringIteration):
        # Messages are appended only when their msg_seq becomes the next expected
        # one. Higher seq numbers stay buffered until the missing gap arrives.
        while iteration_state.next_expected_seq in iteration_state.deferred_messages:
            pv_name, value = iteration_state.deferred_messages.pop(iteration_state.next_expected_seq)
            iteration_state.results[pv_name].append(value)
            iteration_state.next_expected_seq += 1

        snapshot.results = iteration_state.results
        snapshot.interation = iteration_state.iteration
        snapshot.timestamp = iteration_state.header_timestamp

    def __refresh_iteration_legacy_fields(self, snapshot: Snapshot, iteration_state: RecurringIteration):
        snapshot.results = iteration_state.results
        snapshot.interation = iteration_state.iteration
        snapshot.timestamp = iteration_state.header_timestamp

    @staticmethod
    def __group_and_enqueue(buffer: "SubmissionBuffer", merge_queue: queue.SimpleQueue) -> None:
        """Group a sealed submission buffer's messages by PV name and enqueue the result (runs in worker thread)."""
        try:
            grouped: Dict[str, List[Any]] = {}
            for pv_name, value in buffer.messages:
                grouped.setdefault(pv_name, []).append(value)
            merge_queue.put((buffer.submission_seq, grouped))
        except Exception as exc:
            # Signal the merge worker that this buffer failed so it does not hang.
            merge_queue.put((buffer.submission_seq, exc))

    @staticmethod
    def __submit_buffer_for_grouping(executor, buffer: "SubmissionBuffer", merge_queue: queue.SimpleQueue) -> None:
        """Mark a buffer as enqueued and submit it for off-thread grouping. No-op if already enqueued."""
        if not buffer.enqueued:
            buffer.enqueued = True
            executor.submit(dml.__group_and_enqueue, buffer, merge_queue)

    def __append_submission_message(
        self,
        executor,
        snapshot: Snapshot,
        iteration_state: RecurringIteration,
        from_topic: str,
        submission_seq: Optional[int],
        msg_seq: int,
        pv_name: str,
        value: Any,
        last_submission_data: bool = False,
    ) -> bool:
        if submission_seq is None:
            self.__invalidate_iteration(
                snapshot,
                iteration_state,
                from_topic,
                "buffered recurring snapshot message is missing submission_seq",
            )
            return False

        if (
            iteration_state.header_submission_seq is not None
            and submission_seq < iteration_state.header_submission_seq
        ):
            self.__invalidate_iteration(
                snapshot,
                iteration_state,
                from_topic,
                f"submission_seq {submission_seq} is before header submission_seq {iteration_state.header_submission_seq}",
            )
            return False

        submission_buffer = iteration_state.submission_buffers.get(submission_seq)
        if submission_buffer is None:
            submission_buffer = SubmissionBuffer(submission_seq=submission_seq)
            iteration_state.submission_buffers[submission_seq] = submission_buffer

        if submission_buffer.last_msg_seq is not None and msg_seq <= submission_buffer.last_msg_seq:
            self.__invalidate_iteration(
                snapshot,
                iteration_state,
                from_topic,
                f"submission_seq {submission_seq} received non-monotonic msg_seq {msg_seq}",
            )
            return False

        submission_buffer.messages.append((pv_name, value))
        submission_buffer.last_msg_seq = msg_seq

        # A submission is complete only when the server explicitly signals it
        # via last_submission_data=True on the last message of the batch.
        if last_submission_data:
            self.__submit_buffer_for_grouping(executor, submission_buffer, iteration_state.merge_queue)

        return True

    def __finalize_recurring_iteration(self, executor, snapshot: Snapshot, from_topic: str):
        iteration_state = snapshot.active_iteration
        use_submission_seq = self.__uses_submission_seq(snapshot)
        if iteration_state is None or not iteration_state.is_complete(use_submission_seq=use_submission_seq):
            return

        if iteration_state.invalid_reason is not None:
            self.__discard_iteration(snapshot, iteration_state)
            return

        if use_submission_seq and iteration_state.header_submission_seq is None:
            self.__invalidate_iteration(
                snapshot,
                iteration_state,
                from_topic,
                "buffered recurring snapshot iteration completed without a header submission_seq",
            )
            return

        # total_messages is used only as a validation aid here. Ordering is driven
        # entirely by msg_seq and the contiguous flush above.
        if iteration_state.total_messages is not None and iteration_state.tail_seq != iteration_state.total_messages:
            logger.warning(
                "Recurring snapshot %s iteration %s tail msg_seq=%s differs from total_messages=%s",
                from_topic,
                iteration_state.iteration,
                iteration_state.tail_seq,
                iteration_state.total_messages,
            )

        if iteration_state.total_messages is not None:
            expected_data_messages = max(iteration_state.total_messages - 2, 0)
            if use_submission_seq:
                received_data_messages = sum(
                    len(buffer.messages) for buffer in iteration_state.submission_buffers.values()
                )
            else:
                received_data_messages = sum(len(values) for values in iteration_state.results.values()) + len(iteration_state.deferred_messages)
            if received_data_messages < expected_data_messages:
                logger.warning(
                    "Recurring snapshot %s iteration %s incomplete: received %s/%s data messages",
                    from_topic,
                    iteration_state.iteration,
                    received_data_messages,
                    expected_data_messages,
                )
                return

        if use_submission_seq:
            if iteration_state.tail_submission_seq is not None:
                highest_submission_seq = max(iteration_state.submission_buffers, default=iteration_state.header_submission_seq)
                if highest_submission_seq is not None and highest_submission_seq > iteration_state.tail_submission_seq:
                    self.__invalidate_iteration(
                        snapshot,
                        iteration_state,
                        from_topic,
                        f"observed submission_seq {highest_submission_seq} after tail submission_seq {iteration_state.tail_submission_seq}",
                    )
                    return

            captured_buffers = iteration_state.submission_buffers
            captured_iter = iteration_state
            merge_queue = iteration_state.merge_queue
            pv_list = snapshot.pv_list
            handler = snapshot.handler
            expected_count = len(captured_buffers)

            logger.debug(
                f"recurring snapshot {from_topic} tail received [ state {snapshot.state}] "
                f"from {len(pv_list)} PVs with "
                f"{sum(len(b.messages) for b in captured_buffers.values())} "
                f"messages on iteration {captured_iter.iteration}"
            )

            # Enqueue any buffers not yet submitted for grouping
            for buf in captured_buffers.values():
                self.__submit_buffer_for_grouping(executor, buf, merge_queue)

            def _merge_worker():
                # Collect all grouped buffers from the queue (grouping runs in parallel
                # in the thread pool), then sort by submission_seq and merge into results.
                items = []
                for _ in range(expected_count):
                    seq, grouped_or_exc = merge_queue.get()
                    if isinstance(grouped_or_exc, Exception):
                        logger.error("Grouping failed for submission_seq %s in %s: %s", seq, from_topic, grouped_or_exc)
                        return
                    items.append((seq, grouped_or_exc))
                items.sort(key=lambda x: x[0])
                results = {pv: [] for pv in pv_list}
                for _seq, grouped in items:
                    for pv_name, values in grouped.items():
                        results[pv_name].extend(values)
                handler_data = {
                    "iteration":        captured_iter.iteration,
                    "header_timestamp": captured_iter.header_timestamp,
                    "tail_timestamp":   captured_iter.tail_timestamp,
                    "timestamp":        captured_iter.tail_timestamp,
                }
                for pv_name in pv_list:
                    handler_data[pv_name] = results.get(pv_name, [])
                try:
                    handler(from_topic, handler_data)
                except Exception:
                    logger.exception("Handler raised an exception for snapshot %s", from_topic)

            threading.Thread(target=_merge_worker, daemon=True).start()
            self.__promote_next_iteration(snapshot)
            return

        handler_data = {
            "iteration": iteration_state.iteration,
            "header_timestamp": iteration_state.header_timestamp,
            "tail_timestamp": iteration_state.tail_timestamp,
            "timestamp": iteration_state.tail_timestamp,
        }
        for pv_name in snapshot.pv_list:
            handler_data[pv_name] = iteration_state.results.get(pv_name, [])

        logger.debug(
            f"recurring snapshot {from_topic} tail received [ state {snapshot.state}] "
            f"fromm {len(handler_data)} PVs with {sum(len(v) for v in iteration_state.results.values())} "
            f"messages on iteration {iteration_state.iteration}"
        )
        executor.submit(snapshot.handler, from_topic, handler_data)
        self.__promote_next_iteration(snapshot)

    def __handle_recurring_snapshot_header(self, snapshot, from_topic: str, decoded_message: dict, message_iteration: int):
        """Handle recurring snapshot header message (type 0)."""
        header_timestamp = decoded_message.get('timestamp')
        header_submission_seq = decoded_message.get('submission_seq')
        active_iteration = snapshot.active_iteration

        if active_iteration is None:
            self.__set_active_iteration(snapshot, snapshot.create_iteration(message_iteration, header_timestamp))
            if self.__uses_submission_seq(snapshot):
                snapshot.active_iteration.header_submission_seq = header_submission_seq
                if header_submission_seq is None:
                    self.__invalidate_iteration(snapshot, snapshot.active_iteration, from_topic, "buffered recurring snapshot header is missing submission_seq")
            logger.debug(f"recurring snapshot {from_topic} header received [ state {snapshot.state}] and iteration {snapshot.interation}")
            return

        if message_iteration == active_iteration.iteration:
            if active_iteration.header_timestamp is None:
                active_iteration.header_timestamp = header_timestamp
                snapshot.timestamp = header_timestamp
            if self.__uses_submission_seq(snapshot) and active_iteration.header_submission_seq is None:
                active_iteration.header_submission_seq = header_submission_seq
                if header_submission_seq is None:
                    self.__invalidate_iteration(snapshot, active_iteration, from_topic, "buffered recurring snapshot header is missing submission_seq")
            return

        if message_iteration == active_iteration.iteration + 1:
            if snapshot.next_iteration is None or snapshot.next_iteration.iteration != message_iteration:
                snapshot.next_iteration = snapshot.create_iteration(message_iteration, header_timestamp)
            elif snapshot.next_iteration.header_timestamp is None:
                snapshot.next_iteration.header_timestamp = header_timestamp
            if self.__uses_submission_seq(snapshot):
                snapshot.next_iteration.header_submission_seq = header_submission_seq
                if header_submission_seq is None:
                    self.__invalidate_iteration(snapshot, snapshot.next_iteration, from_topic, "buffered recurring snapshot header is missing submission_seq")
            logger.debug("Cached recurring snapshot %s header for next iteration %s", from_topic, message_iteration)
            return

        logger.debug(
            "Ignoring recurring snapshot %s header for iteration %s while active=%s next=%s",
            from_topic,
            message_iteration,
            active_iteration.iteration,
            snapshot.next_iteration.iteration if snapshot.next_iteration else None,
        )

    def __handle_recurring_snapshot_data(self, executor, snapshot, from_topic: str, decoded_message: dict, message_iteration: int):
        """Handle recurring snapshot data message (type 1)."""
        recurring_data_metadata_keys = frozenset((
            'timestamp', 'iter_index', 'message_type', 'message-size',
            'msg_seq', 'submission_seq', 'last_submission_data'
        ))

        # Read msg_seq before stripping metadata
        msg_seq = decoded_message.get('msg_seq', 0)
        submission_seq = decoded_message.get('submission_seq')
        last_submission_data = bool(decoded_message.get('last_submission_data', False))

        # Remove metadata from the message
        for key in recurring_data_metadata_keys:
            decoded_message.pop(key, None)

        # Now the remaining key is the pv name
        pv_name, value = self._extract_remaining_dict_item(decoded_message)
        if pv_name not in snapshot.pv_list:
            logger.warning(f"Received data for unexpected PV '{pv_name}' in snapshot {from_topic}")
            return

        active_iteration = snapshot.active_iteration
        if active_iteration is None:
            logger.debug("Ignoring recurring snapshot %s data for iteration %s before header", from_topic, message_iteration)
            return

        if message_iteration == active_iteration.iteration:
            target_iteration = active_iteration
        elif message_iteration == active_iteration.iteration + 1:
            # Cache one future iteration so we can keep consuming overlap without
            # blocking the current iteration finalization path.
            if snapshot.next_iteration is None or snapshot.next_iteration.iteration != message_iteration:
                snapshot.next_iteration = snapshot.create_iteration(message_iteration)
            target_iteration = snapshot.next_iteration
        else:
            logger.debug(f"Ignoring data message from iteration {message_iteration}, current iteration is {snapshot.interation}")
            return

        use_submission_seq = self.__uses_submission_seq(snapshot)
        if use_submission_seq:
            if not self.__append_submission_message(
                executor,
                snapshot,
                target_iteration,
                from_topic,
                submission_seq,
                msg_seq,
                pv_name,
                value,
                last_submission_data=last_submission_data,
            ):
                return
        else:
            if msg_seq < target_iteration.next_expected_seq:
                logger.debug(
                    "Ignoring duplicate/stale data message for snapshot %s iteration %s seq %s",
                    from_topic,
                    message_iteration,
                    msg_seq,
                )
                return

            target_iteration.deferred_messages[msg_seq] = (pv_name, value)

        if target_iteration is active_iteration:
            if not use_submission_seq:
                # Try to advance the active iteration immediately after every message
                # instead of sorting the whole buffer when the tail arrives.
                self.__flush_iteration_messages(snapshot, target_iteration)
            self.__update_snapshot_state(snapshot)
            self.__finalize_recurring_iteration(executor, snapshot, from_topic)

    def __handle_recurring_snapshot_tail(self, executor, snapshot, from_topic: str, decoded_message: dict, message_iteration: int):
        """Handle recurring snapshot tail message (type 2)."""
        active_iteration = snapshot.active_iteration
        if active_iteration is None:
            logger.debug("Ignoring recurring snapshot %s tail for iteration %s before header", from_topic, message_iteration)
            return

        if message_iteration == active_iteration.iteration:
            target_iteration = active_iteration
        elif message_iteration == active_iteration.iteration + 1:
            if snapshot.next_iteration is None or snapshot.next_iteration.iteration != message_iteration:
                snapshot.next_iteration = snapshot.create_iteration(message_iteration)
            target_iteration = snapshot.next_iteration
        else:
            logger.debug(f"Ignoring tail message from iteration {message_iteration}, current iteration is {snapshot.interation}")
            return

        target_iteration.tail_timestamp = decoded_message.get('timestamp')
        target_iteration.tail_seq = decoded_message.get('msg_seq')
        target_iteration.total_messages = decoded_message.get('total_messages')
        if self.__uses_submission_seq(snapshot):
            target_iteration.tail_submission_seq = decoded_message.get('submission_seq')
            if target_iteration.tail_submission_seq is None:
                self.__invalidate_iteration(snapshot, target_iteration, from_topic, "buffered recurring snapshot tail is missing submission_seq")
                return
            if (
                target_iteration.header_submission_seq is not None
                and target_iteration.tail_submission_seq < target_iteration.header_submission_seq
            ):
                self.__invalidate_iteration(
                    snapshot,
                    target_iteration,
                    from_topic,
                    f"tail submission_seq {target_iteration.tail_submission_seq} is before header submission_seq {target_iteration.header_submission_seq}",
                )
                return

        if target_iteration is active_iteration:
            # Tail can arrive before some lower seq data. Finalization waits until
            # the missing messages have been flushed in order.
            if not self.__uses_submission_seq(snapshot):
                self.__flush_iteration_messages(snapshot, target_iteration)
            self.__update_snapshot_state(snapshot)
            self.__finalize_recurring_iteration(executor, snapshot, from_topic)

    def __handle_reply_message(self, msg_id: str, decoded_message: dict, from_topic: str):
        """Handle reply messages."""
        logger.debug(f"received reply on topic {from_topic}")
        self.reply_message[msg_id] = decoded_message
        self.reply_wait_condition.notify_all()

    def __handle_monitor_event(self, executor, from_topic: str, msg_id: str, decoded_message: dict):
        """Handle monitor events by submitting to thread pool."""
        executor.submit(
            self.process_event,
            from_topic,
            msg_id,
            decoded_message[msg_id]
        )

    def __handle_snapshot_message(self, executor, from_topic: str, msg_id: str, decoded_message: dict):
        """Handle snapshot messages (both regular and recurring)."""
        # Check if it's a regular snapshot
        if msg_id in self.reply_snapsthot_message:
            self.__handle_regular_snapshot(executor, msg_id, decoded_message)
        # Check if it's a recurring snapshot
        elif from_topic in self.reply_recurring_snapsthot_message:
            self.__handle_recurring_snapshot(executor, from_topic, decoded_message)

    def __handle_regular_snapshot(self, executor, msg_id: str, decoded_message: dict):
        """Handle regular snapshot messages."""
        snapshot_metadata_keys = frozenset(('error', 'reply_id', 'message-size', 'msg_seq'))
        snapshot = self.reply_snapsthot_message[msg_id]
        error = decoded_message.get('error', 0)

        if error == 0:
            logger.debug(f"Added message to snapshot {msg_id}]")
            # Remove metadata from the message
            for key in snapshot_metadata_keys:
                decoded_message.pop(key, None)
            # Now the remaining key is the pv name
            pv_name, value = self._extract_remaining_dict_item(decoded_message)
            if pv_name not in snapshot.results:
                snapshot.results[pv_name] = []
            snapshot.results[pv_name].append(value)
        else:
            logger.debug(f"Snapshot {msg_id} compelted with error {error}")
            # we got the completion message so remove the snapshot from the list
            del self.reply_snapsthot_message[msg_id]
            # and call async handler in another thread
            executor.submit(
                snapshot.handler,
                msg_id,
                snapshot.results
            )

    def __handle_recurring_snapshot(self, executor, from_topic: str, decoded_message: dict):
        """Handle recurring snapshot messages with dispatch pattern."""
        snapshot = self.reply_recurring_snapsthot_message[from_topic]
        message_type = decoded_message.get('message_type')
        if message_type is None:
            return

        message_iteration = decoded_message.get('iter_index', 0)

        # Dictionary dispatch pattern (Python 3.9+ alternative to switch/case)
        handlers = {
            0: lambda: self.__handle_recurring_snapshot_header(snapshot, from_topic, decoded_message, message_iteration),
            1: lambda: self.__handle_recurring_snapshot_data(executor, snapshot, from_topic, decoded_message, message_iteration),
            2: lambda: self.__handle_recurring_snapshot_tail(executor, snapshot, from_topic, decoded_message, message_iteration),
        }

        handler = handlers.get(message_type)
        if handler:
            handler()
        else:
            logger.error(f"Error during snapshot {from_topic} with message type {message_type} and state {snapshot.state} and iteration {snapshot.interation} and timestamp {snapshot.timestamp}")

    def __prune_timed_out_replies(self):
        """Remove old timed-out reply IDs to avoid unbounded growth."""
        now_ts = datetime.datetime.now().timestamp()
        expired = [
            rid for rid, timeout_ts in self.__timed_out_replies.items()
            if (now_ts - timeout_ts) > self.__timed_out_replies_ttl_sec
        ]
        for rid in expired:
            self.__timed_out_replies.pop(rid, None)

    def __consumer_handler(self):
        """ Consume message form kafka consumer
        after the message has been consumed the header 'k2eg-ser-type' is checked
        for find the serialization:
            json,
            msgpack,
            msgpack-compact
        """
        with ThreadPoolExecutor(max_workers=10) as executor:
            while self.__consume_data:
                message = self.__broker.get_next_message(self.poll_timeout)
                if message is None:
                    continue

                err = message.error()
                if err:
                    if err.code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.error(
                            f"{message.topic()} [{message.partition()}]reached "+
                            f"end at offset {message.offset()}"
                        )
                    continue

                from_topic = message.topic()
                #msg_id could be a reply id or pv name
                msg_id, decoded_message = self.__decode_message(message)
                if msg_id is None or decoded_message is None:
                    continue

                with self.reply_wait_condition:
                    if msg_id in self.reply_message:
                        self.__handle_reply_message(msg_id, decoded_message, from_topic)
                    elif msg_id in self.__monitor_pv_handler:
                        self.__handle_monitor_event(executor, from_topic, msg_id, decoded_message)
                    elif msg_id in self.reply_snapsthot_message or from_topic in self.reply_recurring_snapsthot_message:
                        self.__handle_snapshot_message(executor, from_topic, msg_id, decoded_message)
                    else:
                        reply_id = decoded_message.get('reply_id') if isinstance(decoded_message, dict) else None
                        msg_keys = list(decoded_message.keys()) if isinstance(decoded_message, dict) else []
                        if reply_id is not None:
                            timeout_ts = self.__timed_out_replies.pop(reply_id, None)
                            if timeout_ts is not None:
                                late_by_sec = datetime.datetime.now().timestamp() - timeout_ts
                                logger.warning(
                                    "Dropped late reply from Kafka: topic=%s reply_id=%s arrived %.3fs after client timeout. keys=%s",
                                    from_topic,
                                    reply_id,
                                    late_by_sec,
                                    msg_keys,
                                )
                            else:
                                logger.warning(
                                    "Dropped unmatched reply from Kafka: topic=%s reply_id=%s keys=%s",
                                    from_topic,
                                    reply_id,
                                    msg_keys,
                                )
                        else:
                            logger.debug(
                                "Dropped unhandled Kafka message: topic=%s msg_id=%s keys=%s",
                                from_topic,
                                msg_id,
                                msg_keys,
                            )


    def parse_pv_url(self, pv_url):
        protocol, pv_name = _filter_pv_uri(pv_url)
        if protocol is None  or pv_name is None:
            raise ValueError(
                "The url is not well formed"
            )
        return protocol, pv_name

    def __check_pv_name(self, pv_url):
        pass

    def _check_pv_list(self, pv_uri_list: list[str]):
        for pv_uri in pv_uri_list:
            protocol, pv_name = self.parse_pv_url(pv_uri)
            if protocol.lower() not in ("pva", "ca"):
                raise ValueError("The protocol need to be one of 'pva'  'ca'")
            
    def __normalize_pv_name(self, pv_name):
        return pv_name.replace(":", "_")

    def _validate_snapshot_name(self, snapshot_name: str) -> None:
        """
        Validate the snapshot name. Only alphanumeric characters, dashes, and underscores are allowed.

        Args:
            snapshot_name (str): The snapshot name to validate.

        Raises:
            ValueError: If the snapshot name contains invalid characters.
        """
        if not re.match(r'^[A-Za-z0-9_\-]+$', snapshot_name):
            raise ValueError(
                f"Invalid snapshot name '{snapshot_name}'. Only alphanumeric characters, dashes, and underscores are allowed."
            )

    def __wait_for_reply(self, new_reply_id, timeout) -> tuple[int, any]:
        #with self.reply_wait_condition:
        got_it = self.reply_wait_condition.wait_for(
            lambda: new_reply_id in self.reply_message and self.reply_message[new_reply_id] is not None,
            timeout
        )
        if not got_it:
            # The timeout has expired and no message was received
            self.__timed_out_replies[new_reply_id] = datetime.datetime.now().timestamp()
            self.__prune_timed_out_replies()
            logger.warning(
                "Timeout waiting for reply_id=%s (timeout=%s). pending_reply_slots=%s",
                new_reply_id,
                timeout,
                len(self.reply_message),
            )
            return -2, None
        reply_msg = self.reply_message.pop(new_reply_id, None)
        if reply_msg is None:
            # This should not occur due to the lambda check, but added as a safety net
            return -1, None
        error = reply_msg.get('error', 0)
        if error != 0:
            str_msg = reply_msg.get('message', None)
            raise OperationError(error, str_msg)
        return 0, reply_msg

    def wait_for_backends(self):
        logger.debug("Waiting for join kafka reply topic")
        self.__broker.wait_for_reply_available()

    def get(self, pv_url: str, timeout: float = None):
        """ Perform the get operation
            raise OperationTimeout when timeout has expired
        """
        protocol, pv_name = self.parse_pv_url(pv_url)
        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The protocol need to be one of 'pva'  'ca'")
        
        new_reply_id = str(uuid.uuid1())
        fetched = False
        result = None
        with self.reply_wait_condition:
            # clear the reply message for the requested pv
            self.reply_message[new_reply_id] = None
            # send message to k2eg
            self.__broker.send_get_command(
                pv_url,
                new_reply_id
            )
            while(not fetched):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout during get operation for {pv_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    fetched = True
        if result is not None and pv_name in result:
            return result[pv_name]
        else:
            return result
                
    def put(self, pv_url: str, value: MessagePackSerializable, timeout: float = None):
        """ Set the value for a single pv
        Args:
            pv_name   (str): is the name of the pv
            value     (str): is the new value
            protocol  (str): the protocol of the pv, the default is pva
            timeout (float): the timeout, in second or fraction
        Raises:
            ValueError: if some parameter are not valid
        
            return the error code and a message in case the error code is not 0
        """
        protocol, pv_name = self.parse_pv_url(pv_url)
        if protocol.lower() not in ("pva", "ca"):
            raise ValueError("The protocol need to be one of 'pva'  'ca'")

        # wait for consumer joined the topic
        fetched = False
        new_reply_id = str(uuid.uuid1())
        logger.info("Send and wait for message")
        with self.reply_wait_condition:
            # init reply slot
            self.reply_message[new_reply_id] = None
            # send message to k2eg
            self.__broker.send_put_command(
                pv_url,
                value.to_base_64(),
                new_reply_id
            )
            while(not fetched):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout during put operation for {pv_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    return result
    

    def monitor(self, pv_url: str, handler: Callable[[str, dict], None], timeout: float = None):  # noqa: E501
        """ Add a new monitor for pv if it is not already activated
        Parameters
                ----------
                pv_name : str
                    The name of the PV to monitor
                handler: function
                    The handler to be called when a message is received
        Rais:
                ----------
                True: the monitor has been activated
                False: otherwhise
        """
        fetched = False
        protocol, pv_name = self.parse_pv_url(pv_url)
        if protocol.lower() not in ("pva", "ca"):
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        new_reply_id = str(uuid.uuid1())
        with self.reply_wait_condition:
            # init reply slot
            self.reply_message[new_reply_id] = None
            if pv_name in self.__monitor_pv_handler:
                logger.info(
                    f"Monitor already activate for pv {pv_name}")
                return
            # send message to k2eg from activate (only for last topics) 
            # monitor(just in case it is not already activated)
            self.__broker.send_start_monitor_command(
                pv_url,
                self.__normalize_pv_name(pv_name),
                new_reply_id,
            )

            while(not fetched):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout during start monitor operation for {pv_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    # all is gone ok i can register the handler and subscribe
                    self.__monitor_pv_handler[pv_name] = handler
                    self.__broker.add_topic(self.__normalize_pv_name(pv_name))
                    return result
        
    def monitor_many(self, pv_uri_list: list[str], handler: Callable[[str, dict], None], timeout: float = None):  # noqa: E501
        """ Add a new monitor for pv if it is not already activated
        Parameters
                ----------
                pv_uri_list : list[str]
                    The name of the PV to monitor
                handler: function
                    The handler to be called when a message is received
        Rais:
                ----------
                True: the monitor has been activated
                False: otherwhise
        """
        fetched = False
        self._check_pv_list(pv_uri_list)
        new_reply_id = str(uuid.uuid1())
        with self.reply_wait_condition:
            filtered_list_pv_uri = []
            # init reply slot
            for pv_uri in pv_uri_list:
                protocol, pv_name = self.parse_pv_url(pv_uri)
                self.reply_message[new_reply_id] = None
                if pv_name in self.__monitor_pv_handler:
                    logger.info(
                        f"Monitor already activate for pv {pv_name}")
                    continue
                filtered_list_pv_uri.append(pv_uri)
            
            if len(filtered_list_pv_uri)==0:
                return
            # send message to k2eg from activate (only for last topics) 
            # monitor(just in case it is not already activated)
            self.__broker.send_start_monitor_command_many(
                filtered_list_pv_uri,
                new_reply_id,
            )

            while(not fetched):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout during start monitor operation for {pv_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    # all is gone ok i can register the handler and subscribe
                    for pv_uri in filtered_list_pv_uri:
                        protocol, pv_name = self.parse_pv_url(pv_uri)
                        self.__monitor_pv_handler[pv_name] = handler
                        self.__broker.add_topic(self.__normalize_pv_name(pv_name))
                    return result
    
    def stop_monitor(self, pv_name: str):  # noqa: E501
        """ Remove movitor for a specific pv
        Parameters
                ----------
                pv_name : str
                    The name of the PV to monitor
        """
        with self.reply_wait_condition:
            # all is gone ok i can register the handler and subscribe
            del self.__monitor_pv_handler[pv_name]
            self.__broker.remove_topic(self.__normalize_pv_name(pv_name))

    def snapshot(self,  pv_uri_list: list[str], handler: Callable[[str, dict], None])->str:
        """ Perform the snapshot creation
        return the id to be used to match the snapthot returned asynchronously in the hanlder
        """
        #check if all the pv are wellformed
        self._check_pv_list(pv_uri_list)
        new_reply_id = str(uuid.uuid1())
        with self.reply_wait_condition:
            # Set the snapshot handler and initialize the snapshot results vector
            self.reply_snapsthot_message[new_reply_id] = Snapshot(handler=handler)

            # send message to k2eg fto execute snapshot
            self.__broker.send_snapshot_command(
                pv_uri_list,
                new_reply_id,
            )
        return new_reply_id
    
    def snapshot_recurring(self,  properties: SnapshotProperties, handler: Callable[[str, Dict[str, Any]], None], timeout: float = None):
        """
        Create a new recurring snapshot for a list of process variables (PVs).

        This method initiates a recurring snapshot operation for the specified PVs.
        It registers a handler to be called asynchronously when snapshot data is available.
        The method blocks until the snapshot is created and an acknowledgment is received from the server,
        or until the specified timeout is reached.

        Args:
            snapshot_name (str): The name to assign to the recurring snapshot.
            pv_uri_list (list[str]): List of PV URIs to include in the snapshot.
            handler (Callable[[str, dict], None]): Callback function to handle snapshot results.
                The handler receives the snapshot ID and a dictionary containing the snapshot data.
            timeout (float, optional): Maximum time to wait for the server acknowledgment, in seconds.
                If None, waits indefinitely.

        Returns:
            str: "ok" if the snapshot is successfully created and acknowledged.

        Raises:
            ValueError: If any PV URI is not well-formed or uses an unsupported protocol.
            OperationTimeout: If the operation times out before receiving an acknowledgment.
            OperationError: If the server returns an error during snapshot creation.

        Example:
            def my_handler(snapshot_id, data):
                print(f"Snapshot {snapshot_id} data: {data}")

            dml_instance.snapshot_recurring(
                "my_snapshot",
                ["pva://my:pv1", "ca://my:pv2"],
                my_handler,
                timeout=5.0
            )
        """
        #check if all the pv are wellformed
        self._check_pv_list(properties.pv_uri_list)
        self._validate_snapshot_name(properties.snapshot_name)
        new_reply_id = str(uuid.uuid1())
        with self.reply_wait_condition:
            # init reply slot
            self.reply_message[new_reply_id] = None

            # create the snaphsot structure
            s = Snapshot(
                handler=handler,
                pv_list = [ self.parse_pv_url(pv_uri)[1] for pv_uri in properties.pv_uri_list ],
                properties = properties
            )
            s.init()
            
            # send message to k2eg fto execute snapshot
            self.__broker.send_repeating_snapshot_command(
                properties,
                new_reply_id,
            )

            while(True):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout during the submition of snapshot {properties.snapshot_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    #at this point we need to start listening to the right topic
                    if "publishing_topic" in result:
                         # Set the snapshot handler and initialize the snapshot results vector
                        s.publishing_topic = result["publishing_topic"]
                        self.reply_recurring_snapsthot_message[s.publishing_topic] = s
                        self.__broker.add_topic(s.publishing_topic)
                        logger.info(
                            f"Recurring snapshot {properties.snapshot_name} listening on topic {s.publishing_topic}"
                        )
                        
                    return result

    def snapshost_trigger(self, snapshot_name: str, timeout: float = None):
        """ Trigger a new publishing of a specific snapshot
        """
        self._validate_snapshot_name(snapshot_name)
        new_reply_id = str(uuid.uuid1())
        with self.reply_wait_condition:
            # init reply slot
            self.reply_message[new_reply_id] = None

            # send message to k2eg fto execute snapshot
            self.__broker.send_repeating_snapshot_trigger_command(
                snapshot_name,
                new_reply_id,
            )

            while(True):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout during triggering the snapshot {snapshot_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    return result

    def snapshot_stop(self, snapshot_name: str, timeout: float = None):
        """ Stop the snapshot operation
        """
        self._validate_snapshot_name(snapshot_name)
        new_reply_id = str(uuid.uuid1())
        with self.reply_wait_condition:
            # init reply slot
            self.reply_message[new_reply_id] = None
            # the snapshot map key is the kafaka topic directly
            topic_key_for_snapshot_to_remove = None
            for topic, snapshot in self.reply_recurring_snapsthot_message.items():
                if snapshot.properties.snapshot_name == snapshot_name:
                    topic_key_for_snapshot_to_remove = topic
                    break
            if topic_key_for_snapshot_to_remove is not None:
                del self.reply_recurring_snapsthot_message[topic_key_for_snapshot_to_remove]
                self.__broker.remove_topic(topic_key_for_snapshot_to_remove)
            
            # send message to k2eg fto execute snapshot
            self.__broker.send_repeating_snapshot_stop_command(
                snapshot_name,
                new_reply_id,
            )

            while(True):
                op_res, result =  self.__wait_for_reply(new_reply_id, timeout)
                if op_res == -2:
                    # raise timeout exception
                    raise OperationTimeout(
                            f"Timeout stopping the snapshot {snapshot_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    return result

    def snapshot_sync(self,  pv_uri_list: list[str], timeout: float = 10.0)->list[dict[str, Any]]:
        """ Perform the snapshot operation
        return the snapshot value synchronously
        """
        snapshot_id = None
        received_snapshot = None
        #check if all the pv are wellformed
        def internal_snapshot_handler(id, snapshot_data):
            nonlocal snapshot_id
            nonlocal received_snapshot
            if snapshot_id == id:
                received_snapshot = snapshot_data
        snapshot_id = self.snapshot(pv_uri_list, internal_snapshot_handler)     
        # wait for received_snapshot isnot None of timeout expired
        
        while(received_snapshot is None):
            # wait some millisecondos on this thread
            sleep(0.3)
            if timeout is not None:
                timeout = timeout - 0.3
                if timeout <= 0:
                    raise OperationTimeout(
                        f"Timeout during snapshot operation for {pv_uri_list}"
                    )
        received_snapshot['error'] = 0
        return received_snapshot

    def close(self):
        # signal thread to terminate
        if self.__thread is not None:
            self.__consume_data = False
            self.__thread.join()
        if self.__broker is not None:
            self.__broker.close()
