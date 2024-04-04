import re
import uuid
import msgpack
import logging
import threading
from readerwriterlock import rwlock
from confluent_kafka import KafkaError
from typing import Callable
from k2eg.broker import Broker
from concurrent.futures import ThreadPoolExecutor

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

class dml:
    """K2EG client"""
    def __init__(
            self, 
            environment_id: str, 
            app_name: str):
        if app_name is None:
            raise ValueError(
                "The app name is mandatory"
            )
        self.__broker = None
        self.__thread = None
        self.__broker = Broker(environment_id, app_name)
        self.__lock = rwlock.RWLockFairD()
        self.__reply_partition_assigned = threading.Event()
        
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
                message = self.__broker.get_next_message()
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

    def parse_pv_url(self, pv_url):
        protocol, pv_name = _filter_pv_uri(pv_url)
        if protocol is None  or pv_name is None:
            raise ValueError(
                "The url is not well formed"
            )
        return protocol, pv_name

    def __check_pv_name(self, pv_url):
        pass

    def __normalize_pv_name(self, pv_name):
        return pv_name.replace(":", "_")

    def __wait_for_reply(self, new_reply_id, timeout) -> (int, any):
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
                
    def put(self, pv_url: str, value: any, timeout: float = None):
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
                value,
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
        for pv_uri in pv_uri_list:
            protocol, pv_name = self.parse_pv_url(pv_uri)
            if protocol.lower() not in ("pva", "ca"):
                raise ValueError("The protocol need to be one of 'pva'  'ca'")
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

    def close(self):
        # signal thread to terminate
        if self.__thread is not None:
            self.__consume_data = False
            self.__thread.join()
        if self.__broker is not None:
            self.__broker.close()
