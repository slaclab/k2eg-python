import re
import uuid
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
from typing import Callable, List, Dict, Any

logger = logging.getLogger(__name__) 
_protocol_regex = r"^(pva?|ca)://((?:[A-Za-z0-9-_:]+(?:\.[A-Za-z0-9-_]+)*))$"

def _filter_pv_uri(uri: str):
    """Parse and validate a PV URI.

    Accepts URIs in the form:
      - `pva://<pv_name>`
      - `ca://<pv_name>`

    Returns the protocol and resource if valid; otherwise `(None, None)`.

    Args:
        uri: The PV URI to validate.

    Returns:
        tuple[str|None, str|None]: `(protocol, resource)` when valid; otherwise `(None, None)`.
    """
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
    """State machine for recurring snapshot assembly."""
    INITIALIZED = 0
    HEADER_RECEVED = 1
    DATA_ACQUIRING = 2
    TAIL_RECEIVED = 3

@dataclass
class Snapshot:
    """Holds state and buffers for snapshot assembly.

    Attributes:
        handler: Callback invoked with snapshot results when ready.
        properties: Optional `SnapshotProperties` used to configure recurring snapshots.
        publishing_topic: Kafka topic used for recurring snapshots.
        state: Current `SnapshotState` of a recurring snapshot.
        timestamp: Timestamp from the header message (recurring snapshots).
        interation: Current iteration index for recurring snapshots.
        pv_list: List of PV names expected in recurring snapshots.
        results: Aggregated values per PV name.
    """
    handler: Callable[[str, Dict[str, Any]], None]
    properties: SnapshotProperties = None
    publishing_topic: str = None
    state: SnapshotState = SnapshotState.INITIALIZED 
    timestamp: datetime.datetime = None
    interation: int = 0
    pv_list: List[str] = field(default_factory=list)
    results: Dict[str, List[Any]] = field(default_factory=dict[str, List[Any]])
    def init(self):
        """Initialize the results dict with empty lists per PV in `pv_list`."""
        for pv in self.pv_list:
            self.results[pv] = []
            
    def clear(self):
        """Clear all lists in the results dictionary without removing keys."""
        for key in self.results:
            self.results[key] = []
            
class dml:
    """K2EG client.

    Provides high-level operations to interact with the K2EG gateway via Kafka:
    - `get`/`put`
    - `monitor`/`monitor_many` and `stop_monitor`
    - one-shot `snapshot` and synchronous `snapshot_sync`
    - `snapshot_recurring`, `snapshost_trigger`, and `snapshot_stop`
    """
    def __init__(
            self, 
            environment_id: str, 
            app_name: str,
            group_name: str = None,
            poll_timeout: float = 0.01):
        """Create a new client instance.

        Args:
            environment_id: Environment identifier used to load Kafka configuration.
            app_name: Logical application name; also used to build the reply topic.
            group_name: Optional consumer group; defaults to `app_name` if not provided.
            poll_timeout: Poll timeout (seconds) for the consumer thread.

        Raises:
            ValueError: If `app_name` is not provided.
        """
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
        """Decode a JSON-encoded payload.

        Not implemented in this client version. Present for future parity with
        other serialization formats.
        """
        print('__from_json')

    def __from_msgpack(self, m_msg):
        """Decode a MessagePack-encoded payload.

        Extracts a message id from well-known keys in the decoded dictionary and
        annotates the payload with the raw message size under `message-size`.

        Args:
            m_msg: Raw MessagePack bytes.

        Returns:
            tuple[str|None, dict|None]: `(msg_id, decoded_payload)` or `(None, None)`.
        """
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
        """Decode a compact-MessagePack payload.

        Not implemented in this client version. Present for format completeness.
        """
        print('__from_msgpack_compack')

    def __decode_message(self, msg):
        """Decode a single Kafka message based on `k2eg-ser-type` header.

        Supports `json`, `msgpack`, and `msgpack-compact` (placeholders for json/compact).

        Args:
            msg: The Kafka message object.

        Returns:
            tuple[str|None, dict|None]: `(msg_id, decoded_payload)` or `(None, None)`.
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
        return msg_id, converted_msg

    def process_event(self, topic_name, msg_id, decoded_message):
        """Dispatch a decoded monitor event to the registered handler.

        Args:
            topic_name: Kafka topic where the event was received.
            msg_id: PV name (monitor key).
            decoded_message: Decoded payload for the PV.
        """
        logger.debug(f"received event on topic {topic_name}")
        self.__monitor_pv_handler[msg_id](msg_id, decoded_message)
    
    def __consumer_handler(self):
        """Background consumer loop.

        Continuously polls Kafka, decodes messages according to `k2eg-ser-type`,
        and routes them to one of the following flows:
        - Reply handling (condition variable signaling)
        - Monitor event dispatch (per-PV handlers)
        - One-shot snapshot aggregation
        - Recurring snapshot assembly and handler dispatch
        """
        with  ThreadPoolExecutor(max_workers=10) as executor:
            while self.__consume_data:
                try:
                    message = self.__broker.get_next_message(self.poll_timeout)
                    if message is None:
                        continue
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event; not an error for us
                            logger.debug(
                                f"{message.topic()} [{message.partition()}] reached end at offset {message.offset()}"
                            )
                        # Skip errored messages
                        continue

                    from_topic = message.topic()
                    # msg_id could be a reply id, snapshot name, or pv name
                    msg_id, decoded_message = self.__decode_message(message)
                    if msg_id is None or decoded_message is None:
                        continue

                    # 1) Fast path for replies (use the condition only here)
                    with self.reply_wait_condition:
                        if msg_id in self.reply_message:
                            logger.debug(f"received reply on topic {from_topic}")
                            self.reply_message[msg_id] = decoded_message
                            self.reply_wait_condition.notify_all()
                            # handled as reply; move to next message
                            continue

                    # 2) Monitor events (no need to hold the reply condition)
                    try:
                        # Read lock to safely check handler presence
                        with self.__lock.gen_rlock():
                            monitor_handler_present = msg_id in self.__monitor_pv_handler
                            monitor_handler = self.__monitor_pv_handler.get(msg_id)
                        if monitor_handler_present and monitor_handler is not None:
                            # Prefer the pv-named payload; fall back defensively if missing
                            payload = decoded_message.get(
                                msg_id,
                                next((v for k, v in decoded_message.items() if k not in (
                                    'reply_id', 'message-size', 'error', 'snapshot_name', 'timestamp', 'message_type', 'iter_index'
                                )), None)
                            )
                            if payload is not None:
                                executor.submit(self.process_event, from_topic, msg_id, payload)
                                continue
                    except Exception as e:
                        logger.exception(f"Error dispatching monitor event for {msg_id}: {e}")

                    # 3) One-shot snapshot aggregation (keyed by snapshot id)
                    with self.__lock.gen_rlock():
                        snapshot = self.reply_snapsthot_message.get(msg_id)
                    if snapshot is not None:
                        # check if the message is a snapshot completion error == 1
                        if decoded_message.get('error', 0) == 0:
                            logger.debug(f"Added message to snapshot {msg_id}")
                            # Remove metadata and keep only pv payload
                            for k in ('error', 'reply_id', 'message-size'):
                                decoded_message.pop(k, None)
                            if len(decoded_message) == 1:
                                pv_name, value = next(iter(decoded_message.items()))
                                with self.__lock.gen_wlock():
                                    if pv_name not in snapshot.results:
                                        snapshot.results[pv_name] = []
                                    snapshot.results[pv_name].append(value)
                        else:
                            logger.debug(
                                f"Snapshot {msg_id} completed with error {decoded_message.get('error', 0)}"
                            )
                            # Completion: remove snapshot and invoke handler asynchronously
                            with self.__lock.gen_wlock():
                                self.reply_snapsthot_message.pop(msg_id, None)
                            executor.submit(snapshot.handler, msg_id, snapshot.results)
                        continue

                    # 4) Recurring snapshot stream (keyed by publishing topic)
                    with self.__lock.gen_rlock():
                        r_snapshot = self.reply_recurring_snapsthot_message.get(from_topic)
                    if r_snapshot is not None:
                        message_type = decoded_message.get('message_type', None)
                        if message_type is None:
                            continue

                        # Get the current iteration from the message
                        message_iteration = decoded_message.get('iter_index', 0)

                        if message_type == 0 and (r_snapshot.state == SnapshotState.INITIALIZED or r_snapshot.state == SnapshotState.TAIL_RECEIVED):
                            with self.__lock.gen_wlock():
                                r_snapshot.state = SnapshotState.HEADER_RECEVED
                                r_snapshot.timestamp = decoded_message.get('timestamp', None)
                                r_snapshot.interation = decoded_message.get('iter_index', 0)
                            logger.debug(
                                f"recurring snapshot {from_topic} header received [state {r_snapshot.state}] iteration {r_snapshot.interation}"
                            )
                            continue

                        if message_type == 1 and (r_snapshot.state == SnapshotState.HEADER_RECEVED or r_snapshot.state == SnapshotState.DATA_ACQUIRING):
                            if message_iteration == r_snapshot.interation:
                                with self.__lock.gen_wlock():
                                    r_snapshot.state = SnapshotState.DATA_ACQUIRING
                                # Remove metadata from the message
                                for k in ('timestamp', 'iter_index', 'message_type', 'message-size'):
                                    decoded_message.pop(k, None)
                                # Now the remaining key is the pv name
                                if len(decoded_message) == 1:
                                    pv_name, value = next(iter(decoded_message.items()))
                                    with self.__lock.gen_wlock():
                                        if pv_name not in r_snapshot.results:
                                            r_snapshot.results[pv_name] = []
                                        r_snapshot.results[pv_name].append(value)
                            else:
                                logger.debug(
                                    f"Ignoring data message from iteration {message_iteration}, current iteration is {r_snapshot.interation}"
                                )
                            continue

                        if message_type == 2 and (r_snapshot.state == SnapshotState.HEADER_RECEVED or r_snapshot.state == SnapshotState.DATA_ACQUIRING):
                            if message_iteration == r_snapshot.interation:
                                with self.__lock.gen_wlock():
                                    r_snapshot.state = SnapshotState.TAIL_RECEIVED
                                    tail_ts = decoded_message.get('timestamp', None)
                                    handler_data = {
                                        "iteration": r_snapshot.interation,
                                        "header_timestamp": r_snapshot.timestamp,
                                        "tail_timestamp": tail_ts,
                                        "timestamp": tail_ts,
                                    }
                                    for pv_name in r_snapshot.pv_list:
                                        if pv_name in r_snapshot.results:
                                            handler_data[pv_name] = r_snapshot.results[pv_name]
                                logger.debug(
                                    f"recurring snapshot {from_topic} tail received [state {r_snapshot.state}] from {len(handler_data)} PVs with {sum(len(v) for v in r_snapshot.results.values())} messages on iteration {r_snapshot.interation}"
                                )
                                executor.submit(r_snapshot.handler, from_topic, handler_data)
                                with self.__lock.gen_wlock():
                                    r_snapshot.clear()  # Prepare for next iteration
                            else:
                                logger.debug(
                                    f"Ignoring tail message from iteration {message_iteration}, current iteration is {r_snapshot.interation}"
                                )
                            continue

                        # Unexpected state/type combination
                        logger.error(
                            f"Error during snapshot {from_topic} with message type {message_type} and state {r_snapshot.state} and iteration {r_snapshot.interation} and timestamp {r_snapshot.timestamp}"
                        )
                        continue

                    # Nothing matched; drop silently
                except Exception as e:
                    # Ensure the consumer loop is resilient
                    logger.exception(f"Unhandled exception in consumer handler: {e}")



    def parse_pv_url(self, pv_url):
        """Validate and split a PV URL into protocol and name.

        Args:
            pv_url: The PV URL (e.g., `pva://dev:chan` or `ca://PV_NAME`).

        Returns:
            tuple[str, str]: `(protocol, pv_name)`.

        Raises:
            ValueError: If the URL is not well formed.
        """
        protocol, pv_name = _filter_pv_uri(pv_url)
        if protocol is None  or pv_name is None:
            raise ValueError(
                "The url is not well formed"
            )
        return protocol, pv_name

    def __check_pv_name(self, pv_url):
        """Placeholder for PV name validation (reserved for future use)."""
        pass

    def _check_pv_list(self, pv_uri_list: list[str]):
        """Validate a list of PV URIs for supported protocol and format.

        Each PV URI must be prefixed with `pva://` or `ca://`.
        """
        for pv_uri in pv_uri_list:
            protocol, pv_name = self.parse_pv_url(pv_uri)
            if protocol.lower() not in ("pva", "ca"):
                raise ValueError("The protocol need to be one of 'pva'  'ca'")
            
    def __normalize_pv_name(self, pv_name):
        """Normalize a PV name for topic usage (replace ':' with '_')."""
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
        """Block until a reply for `new_reply_id` is available or timeout occurs.

        Args:
            new_reply_id: Correlation id used for matching a reply.
            timeout: Maximum time (seconds) to wait; `None` waits indefinitely.

        Returns:
            tuple[int, Any]: `(-2, None)` on timeout, `(-1, None)` on spurious wake,
            or `(0, reply_dict)` on success. Raises on error in reply.

        Raises:
            OperationError: If the reply contains a non-zero error code.
        """
        #with self.reply_wait_condition:
        got_it = self.reply_wait_condition.wait_for(
            lambda: new_reply_id in self.reply_message and self.reply_message[new_reply_id] is not None,
            timeout
        )
        if not got_it:
            # The timeout has expired and no message was received
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
        """Block until the client joins the reply topic."""
        logger.debug("Waiting for join kafka reply topic")
        self.__broker.wait_for_reply_available()

    def get(self, pv_url: str, timeout: float = None):
        """Retrieve the current value of a PV.

        Args:
            pv_url: PV URL (e.g., `pva://dev:chan`).
            timeout: Optional timeout in seconds.

        Returns:
            dict | Any: The decoded PV value structure, or the raw reply dict.

        Raises:
            ValueError: If the PV URL or protocol is invalid.
            OperationTimeout: If the operation times out.
            OperationError: If the server returns an error.
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
        """Set the value of a PV.

        Args:
            pv_url: PV URL to write.
            value: A MessagePackSerializable payload to write.
            timeout: Optional timeout in seconds.

        Returns:
            dict: Reply dictionary on success.

        Raises:
            ValueError: If parameters are invalid.
            OperationTimeout: If the operation times out.
            OperationError: If the server returns an error.
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
                            f"Timeout during start get operation for {pv_name}"
                            )
                elif op_res == -1:
                    continue
                else:
                    return result
    

    def monitor(self, pv_url: str, handler: Callable[[str, dict], None], timeout: float = None):  # noqa: E501
        """Start a monitor for a PV and invoke a handler on updates.

        Args:
            pv_url: PV URL to monitor.
            handler: Callback `(pv_name, value_dict)` invoked per update.
            timeout: Optional timeout waiting for acknowledgment.

        Returns:
            dict | None: Reply dictionary on success, or `None` if already active.

        Raises:
            ValueError: If the PV URL or protocol is invalid.
            OperationTimeout: If the operation times out.
            OperationError: If the server returns an error.
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
        """Start monitors for multiple PVs and invoke a handler on updates.

        Args:
            pv_uri_list: List of PV URLs to monitor. Each entry must be prefixed with `pva://` or `ca://`.
            handler: Callback `(pv_name, value_dict)` per PV update.
            timeout: Optional timeout waiting for acknowledgment.

        Returns:
            dict | None: Reply dictionary on success, or `None` if nothing to add.

        Raises:
            ValueError: If a PV URL or protocol is invalid.
            OperationTimeout: If the operation times out.
            OperationError: If the server returns an error.
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
        """Stop monitoring a specific PV and unsubscribe from its topic.

        Args:
            pv_name: The PV name (without protocol) previously monitored.
        """
        with self.reply_wait_condition:
            # all is gone ok i can register the handler and subscribe
            del self.__monitor_pv_handler[pv_name]
            self.__broker.remove_topic(self.__normalize_pv_name(pv_name))

    def snapshot(self,  pv_uri_list: list[str], handler: Callable[[str, dict], None])->str:
        """Request a one-shot snapshot and register a handler for results.

        Args:
            pv_uri_list: List of PV URLs to snapshot. Each entry must be prefixed with `pva://` or `ca://`.
            handler: Callback `(snapshot_id, data_dict)` invoked asynchronously.

        Returns:
            str: The generated `snapshot_id` used to correlate results.
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
        """Create a recurring snapshot and register a handler per iteration tail.

        Blocks until the snapshot is created (ack received) or `timeout` expires, then
        starts listening to the provided publishing topic and asynchronously dispatches
        assembled results on each iteration tail to `handler`.

        Args:
            properties: Snapshot configuration (name, list of PVs, timing, etc.). `properties.pv_uri_list` must contain PV URLs prefixed with `pva://` or `ca://`.
            handler: Callback `(publishing_topic, data_dict)` invoked at each tail.
            timeout: Optional timeout waiting for the acknowledgment.

        Returns:
            dict: The acknowledgment reply dictionary.

        Raises:
            ValueError: If input validation fails.
            OperationTimeout: If the operation times out.
            OperationError: If the server returns an error.
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
        """Trigger an on-demand publish for a recurring snapshot."""
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
        """Stop a recurring snapshot and unsubscribe its publishing topic."""
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
        """Perform a one-shot snapshot and return results synchronously.

        Args:
            pv_uri_list: List of PV URLs to snapshot. Each entry must be prefixed with `pva://` or `ca://`.
            timeout: Overall timeout for receiving the assembled snapshot.

        Returns:
            dict[str, Any]: Snapshot data per PV with `error` key set to 0 on success.

        Raises:
            OperationTimeout: If the snapshot assembly times out.
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
        """Close the client, stop the consumer thread, and release resources."""
        # signal thread to terminate
        if self.__thread is not None:
            self.__consume_data = False
            self.__thread.join()
        if self.__broker is not None:
            self.__broker.close()
