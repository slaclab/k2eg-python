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
class Snapshot:
    handler: Callable[[str, Dict[str, Any]], None]
    properties: SnapshotProperties = None
    publishing_topic: str = None
    state: SnapshotState = SnapshotState.INITIALIZED 
    timestamp: datetime.datetime = None
    interation: int = 0
    pv_list: List[str] = field(default_factory=list)
    results: Dict[str, List[Any]] = field(default_factory=dict[str, List[Any]])
    def init(self):
        # fill the results with empty lists for each pv
        for pv in self.pv_list:
            self.results[pv] = []
            
    def clear(self):
        """Clear all lists in the results dictionary without removing the keys."""
        for key in self.results:
            self.results[key] = []
            
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
        #contain a vector for each reply id where snapshot are stored
        self.reply_snapsthot_message = {}
        self.reply_recurring_snapsthot_message = {}
        logging.info(
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
            logging.error("Message without header received")
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
        logging.debug(f"received event on topic {topic_name}")
        self.__monitor_pv_handler[msg_id](msg_id, decoded_message)
    
    def __consumer_handler(self):
        """ Consume message form kafka consumer
        after the message has been consumed the header 'k2eg-ser-type' is checked 
        for find the serialization:
            json, 
            msgpack, 
            msgpack-compact
        """
        with  ThreadPoolExecutor(max_workers=10) as executor:
            while self.__consume_data:
                message = self.__broker.get_next_message(self.poll_timeout)
                if message is None: 
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logging.error(
                            f"{message.topic()} [{message.partition()}]reached "+
                            f"end at offset {message.offset()}"
                        )
                    else:
                        continue
                else:
                    was_a_reply = False
                    from_topic = message.topic()
                    #msg_id could be a reply id or pv name
                    msg_id, decoded_message = self.__decode_message(message)
                    if msg_id is None or decoded_message is None:
                        continue
                    with self.reply_wait_condition:
                        was_a_reply = msg_id in self.reply_message
                        if was_a_reply is True:
                            # print(f"message received from topic: {message.topic()} offset: {message.offset()}")
                            logging.debug(f"received reply on topic {message.topic()}")
                            self.reply_message[msg_id] = decoded_message
                            self.reply_wait_condition.notify_all()
                        elif msg_id in self.__monitor_pv_handler:
                                executor.submit(
                                    self.process_event,
                                    message.topic(),
                                    msg_id,
                                    decoded_message[msg_id]
                                )
                        elif msg_id in self.reply_snapsthot_message:
                            # if the message is not a reply and not a monitor
                            # it should be a snapshot
                            snapshot = self.reply_snapsthot_message[msg_id]
                            # check if the message is a snapshot completion error == 1
                            if decoded_message.get('error', 0) == 0:
                                logging.debug(f"Added message to snapshot {msg_id}]") 
                                decoded_message.pop('error', None)
                                decoded_message.pop('reply_id', None)
                                decoded_message.pop('message-size', None)
                                # the message contains a snapshot value
                                if len(decoded_message) == 1:
                                    pv_name, value = next(iter(decoded_message.items()))
                                    if pv_name not in snapshot.results:
                                        snapshot.results[pv_name] = []
                                    snapshot.results[pv_name].append(value)
                            else:
                                logging.debug(f"Snapshot {msg_id} compelted with error {decoded_message.get('error', 0)}")
                                # we got the completion message so             
                                # remove the snapshot from the list
                                del self.reply_snapsthot_message[msg_id]
                                # and call async handler in another thread
                                executor.submit(
                                    snapshot.handler,
                                    msg_id,
                                    snapshot.results
                                )
                        elif from_topic in self.reply_recurring_snapsthot_message:
                             # it should be a recurring snapshot
                            snapshot = self.reply_recurring_snapsthot_message[from_topic]
                            message_type = decoded_message.get('message_type', None)
                            if message_type is None:
                                continue
                            
                            # Get the current iteration from the message
                            message_iteration = decoded_message.get('iter_index', 0)
                            
                            if message_type == 0 and (snapshot.state == SnapshotState.INITIALIZED or snapshot.state == SnapshotState.TAIL_RECEIVED):
                                snapshot.state = SnapshotState.HEADER_RECEVED
                                # Get the timestamp and iteration
                                snapshot.timestamp = decoded_message.get('timestamp', None)
                                snapshot.interation = decoded_message.get('iter_index', 0)
                                logging.debug(f"recurring snapshot {from_topic} header received [ state {snapshot.state}] and iteration {snapshot.interation}")
                                
                            elif message_type == 1 and (snapshot.state == SnapshotState.HEADER_RECEVED or snapshot.state == SnapshotState.DATA_ACQUIRING):
                                # Only process data messages that match the current iteration
                                if message_iteration == snapshot.interation:
                                    snapshot.state = SnapshotState.DATA_ACQUIRING
                                    # Remove metadata from the message
                                    decoded_message.pop('timestamp', None)
                                    decoded_message.pop('iter_index', None)
                                    decoded_message.pop('message_type', None)
                                    decoded_message.pop('message-size', None)
                                    
                                    # Now the remaining key is the pv name
                                    if len(decoded_message) == 1:
                                        pv_name, value = next(iter(decoded_message.items()))
                                        if pv_name not in snapshot.results:
                                            snapshot.results[pv_name] = []
                                        snapshot.results[pv_name].append(value)
                                    #logging.debug(f"recurring snapshot {from_topic} data received [ state {snapshot.state}] messages {sum(len(v) for v in snapshot.results.values())} and iteration {snapshot.interation}")
                                else:
                                    logging.debug(f"Ignoring data message from iteration {message_iteration}, current iteration is {snapshot.interation}")
                                    
                            elif message_type == 2 and (snapshot.state == SnapshotState.HEADER_RECEVED or snapshot.state == SnapshotState.DATA_ACQUIRING):
                                # Only process tail messages that match the current iteration
                                if message_iteration == snapshot.interation:
                                    snapshot.state = SnapshotState.TAIL_RECEIVED
                                    # Build handler data with metadata
                                    tail_ts = decoded_message.get('timestamp', None)
                                    handler_data = {
                                        "iteration": snapshot.interation,
                                        "header_timestamp": snapshot.timestamp,
                                        "tail_timestamp": tail_ts,
                                        "timestamp": tail_ts,
                                    }
                                    # Add PV data
                                    for pv_name in snapshot.pv_list:
                                        if pv_name in snapshot.results:
                                            handler_data[pv_name] = snapshot.results[pv_name]
                                    
                                    logging.debug(f"recurring snapshot {from_topic} tail received [ state {snapshot.state}] fromm {len(handler_data)} PVs with {sum(len(v) for v in snapshot.results.values())} messages on iteration {snapshot.interation}")
                                    # Call handler asynchronously
                                    executor.submit(
                                        snapshot.handler,
                                        from_topic,
                                        handler_data
                                    )
                                    snapshot.clear()  # Clear results for the next iteration
                                else:
                                    logging.debug(f"Ignoring tail message from iteration {message_iteration}, current iteration is {snapshot.interation}")
                            else:
                                logging.error(f"Error during snapshot {from_topic} with message type {message_type} and state {snapshot.state} and iteration {snapshot.interation} and timestamp {snapshot.timestamp}")



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
        logging.debug("Waiting for join kafka reply topic")
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
        logging.info("Send and wait for message")
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
                logging.info(
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
                    logging.info(
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
                        logging.info(
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
