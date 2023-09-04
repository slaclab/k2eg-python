import re
import uuid
import msgpack
import logging
import threading
from readerwriterlock import rwlock
from confluent_kafka import KafkaError
from typing import Callable
from k2eg.broker import Broker

_protocol_regex = r"(pva?|ca)://([-\w.]+(/[-\w./]*)*:.*)"

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
            app_name: str = str(uuid.uuid1())):
        self.__broker = Broker(environment_id, app_name, app_name)
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

    def __from_json(self, j_msg, is_a_reply: bool):
        print('__from_json')

    def __from_msgpack(self, m_msg, is_a_reply: bool):
        msg_id = None
        converted_msg = None
        decodec_msg = msgpack.loads(m_msg)
        if not isinstance(decodec_msg, dict):
            return msg_id, converted_msg
        
        if is_a_reply:
            msg_id = decodec_msg['reply_id']
            converted_msg = decodec_msg
        else:
            msg_id = list(decodec_msg.keys())[0]
            converted_msg = decodec_msg
        return msg_id, converted_msg

    def __from_msgpack_compack(self, mc_msg, is_a_reply: bool):
        print('__from_msgpack_compack')

    def __decode_message(self, msg, is_reply_msg):
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
                        msg.value(), is_reply_msg
                        )
                elif st == "msgpack":
                    msg_id, converted_msg = self.__from_msgpack(
                        msg.value(), is_reply_msg
                        )
                elif st == "msgpack-compact":
                    msg_id, converted_msg = self.__from_msgpack_compack(
                        msg.value(), is_reply_msg
                        )
                break   
        return msg_id, converted_msg

    def __process_message(self, pv_name, converted_msg):
        """ Process single message
        """
        with self.__lock.gen_rlock():
            if pv_name not in self.__monitor_pv_handler:
                return
            self.__monitor_pv_handler[pv_name](pv_name, converted_msg[pv_name])
            logging.debug(
                f'read message sent to {self.__monitor_pv_handler[pv_name]} handler'
            )

    def __consumer_handler(self):
        """ Consume message form kafka consumer
        after the message has been consumed the header 'k2eg-ser-type' is checked 
        for find the serialization:
            json, 
            msgpack, 
            msgpack-compact
        """
        while self.__consume_data:
        #for msg in self.__consumer:
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
                elif message.error():
                    logging.error(message.error())
            else:
                # good message
                if message.topic() == self.__broker.get_reply_topic():
                    logging.debug(f"received reply with offset {message.offset()}")
                    reply_id, converted_msg = self.__decode_message(message, True)
                    if reply_id is None or converted_msg is None:
                        continue
                    with self.reply_wait_condition:
                        self.reply_message[reply_id] = converted_msg
                        self.reply_wait_condition.notifyAll()
                else:
                    logging.debug(
                        "received monitor message with offset "+
                        f"{message.offset()} from topic {message.topic()}"
                    )
                    pv_name, converted_msg = self.__decode_message(message, False)
                    if pv_name is None or converted_msg is None:
                        continue
                    self.__process_message(pv_name, converted_msg)
                self.__broker.commit_current_fetched_message()

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
        
        # wait for consumer joined the topic
        self.__broker.wait_for_reply_available()
        result = None
        new_reply_id = str(uuid.uuid1())
        fetched = False
        # wait for response
        while(not fetched):
            logging.info("Send and wait for message")
            with self.reply_wait_condition:
                # clear the reply message for the requested pv
                self.reply_message[new_reply_id] = None
                # send message to k2eg
                self.__broker.send_get_command(
                    pv_name,
                    protocol.lower(),
                    new_reply_id
                )
                got_it = self.reply_wait_condition.wait(timeout)
                if(got_it is False):
                    # the timeout is expired, so delete the answer slot
                    # and rise exception
                    del(self.reply_message[new_reply_id])
                    raise OperationTimeout(
                        f"Timeout during get operation for {pv_name}"
                        )
                if self.reply_message[new_reply_id] is None:
                    continue
                fetched = True

                message = None
                result = None
                error = self.reply_message[new_reply_id]['error']
                if 'message' in self.reply_message[new_reply_id]:
                    message = self.reply_message[new_reply_id]['message']
                if error == 0:
                    result = self.reply_message[new_reply_id][pv_name]
                del(self.reply_message[new_reply_id])
                if error != 0:
                    raise OperationError(error, message)
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
        error = 0
        message = None
        fetched = False
        self.__broker.wait_for_reply_available()
        new_reply_id = str(uuid.uuid1())

        logging.info("Send and wait for message")
        with self.reply_wait_condition:
            # init reply slot
            self.reply_message[new_reply_id] = None
                # send message to k2eg
            self.__broker.send_put_command(
                pv_name,
                value,
                protocol.lower(),
                new_reply_id
            )
            while(not fetched):
                got_it = self.reply_wait_condition.wait(timeout)
                if(got_it is False):
                    # the timeout is expired, so delete the answer slot
                    # and rise exception
                    del(self.reply_message[new_reply_id])
                    raise OperationTimeout(
                        f"Timeout during put operation for {pv_name}"
                        )
                if self.reply_message[new_reply_id] is None:
                    continue
                fetched = True
                reply_msg = self.reply_message[new_reply_id]
                message = None
                error = reply_msg['error']
                if 'message' in reply_msg:
                    message = reply_msg['message']   
                del(self.reply_message[new_reply_id])
                if error != 0:
                    raise OperationError(error, message)

    def monitor(self, pv_url: str, handler: Callable[[str, dict], None]):  # noqa: E501
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
        protocol, pv_name = self.parse_pv_url(pv_url)

        if protocol.lower() not in ("pva", "ca"):
            raise ValueError("The portocol need to be one of 'pva'  'ca'")

        with self.__lock.gen_wlock():
            if pv_name in self.__monitor_pv_handler:
                logging.info(
                    f"Monitor already activate for pv {pv_name}")
                return
            self.__monitor_pv_handler[pv_name] = handler
            self.__broker.add_topic(self.__normalize_pv_name(pv_name))

        # send message to k2eg from activate (only for last topics) 
        # monitor(just in case it is not already activated)
        self.__broker.send_start_monitor_command(
            pv_name,
            protocol,
            self.__normalize_pv_name(pv_name)
        )
    
    def stop_monitor(self, pv_name: str):
        """ Stop a new monitor for pv if it is not already activated
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
        self.__check_pv_name(pv_name)

        with self.__lock.gen_wlock():
            if pv_name not in self.__monitor_pv_handler:
                logging.info(
                    f"Monitor already stopped for pv {pv_name}")
                return
            del self.__monitor_pv_handler[pv_name]
            self.__broker.remove_topic(self.__normalize_pv_name(pv_name))

        # send message to k2eg from activate (only for last topics) 
        # monitor(just in case it is not already activated)
        self.__broker.send_stop_monitor_command(
            pv_name,
            self.__normalize_pv_name(pv_name)
        )

    def close(self):
        self.__consume_data = False
        self.__broker.close()
        self.__thread.join()