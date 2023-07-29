import logging
import threading
import uuid
from readerwriterlock import rwlock
from confluent_kafka import KafkaError
import re
import msgpack
from typing import Callable

from .broker import Broker

class OperationTimeout(Exception):
    """Exception raised when the tmeout is epired on operation"""
    def __init__(self, message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class OperationError(Exception):
    """Exception raised when the tmeout is epired on operation"""
    def __init__(self, error, message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.error = error

class dml:
    """K2EG client"""
    def __init__(self, environment_id:str, app_name: str = str(uuid.uuid1())):
        self.__broker = Broker(
            environment_id = environment_id,
            group_name = app_name
            )
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

    def close(self):
        self.__consume_data = False
        self.__thread.join()
        self.__broker.close()

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
            self.__monitor_pv_handler[pv_name](converted_msg)
            logging.debug(
                'read message sent to {} hanlder'
                    .format(self.__monitor_pv_handler[pv_name])
            )

    def __consumer_handler(self):
        """ Consume message form kafka consumer
        after the message has been consumed the header 'k2eg-ser-type' is checked 
        for find the serializaiton:
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
                        '{} [{}]reached end at offset {}'
                        .format(message.topic(), message.partition(), message.offset())
                    )
                elif message.error():
                    logging.error(message.error())
            else:
                # good message
                if message.topic() == self.__broker.get_reply_topic():
                    logging.info("received reply with offset {}".format(message.offset))
                    reply_id, converted_msg = self.__decode_message(message, True)
                    if reply_id is None or converted_msg is None:
                        continue
                    with self.reply_wait_condition:
                        self.reply_message[reply_id] = converted_msg
                        self.reply_wait_condition.notifyAll()
                else:
                    logging.info(
                        "received monitor message with offset {} from topic {}"
                        .format(message.offset, message.topic)
                    )
                    pv_name, converted_msg = self.__decode_message(message, False)
                    if pv_name is None or converted_msg is None:
                        continue
                    self.__process_message(pv_name, converted_msg)
                self.__broker.commit_current_fetched_message()

    def __check_pv_name(self, pv_name):
        pattern = r'^[a-zA-Z0-9:]+$'
        if re.match(pattern, pv_name):
            return True
        else:
            return False

    def __normalize_pv_name(self, pv_name):
        return pv_name.replace(":", "_")

    def with_for_backends(self):
        logging.debug("Waiting for join kafka reply topic")
        self.__broker.wait_for_reply_available()

    def get(self, pv_name: str, protocol: str = 'pva', timeout: float = None):
        """ Perform the get operation
            raise OperationTimeout when timeout has expired
        """
        if not self.__check_pv_name(pv_name):
            raise ValueError(
                "The PV name can only containes letter (upper or lower)"
                ", number ad the character ':'"
            )
        
        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        
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
                        "Timeout during get orpation for {}".format(pv_name)
                        )
                if self.reply_message[new_reply_id] is None:
                    continue
                fetched = True

                message = None
                result = None
                error = self.reply_message[new_reply_id]['error']
                if 'message' in self.reply_message:
                    message = self.reply_message['message']
                if error == 0:
                    result = self.reply_message[new_reply_id][pv_name]
                del(self.reply_message[new_reply_id])
                if error != 0:
                    raise OperationError(error, message)
        return result
                
    def put(self, pv_name: str, value: any, protocol: str = 'pva', timeout: float = None):  # noqa: E501
        """ Set the value for a single pv
        Args:
            pv_name   (str): is the name of the pv
            value     (str): is the new value
            protocol  (str): the protocl of the pv, the default is pva
            timeout (float): the timeout, in second or fraction
        Raises:
            ValueError: if some paramter are not valid
        
            return the error code and a message in case the error code is not 0
        """
        if not self.__check_pv_name(pv_name):
            raise ValueError(
                "The PV name can only containes letter (upper or lower)"
                ", number ad the character ':'"
            )

        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        
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
                        "Timeout during put orpation for {}".format(pv_name)
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

    def monitor(self, pv_name: str, handler: Callable[[any], None], protocol: str = 'pva'):  # noqa: E501
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
        if not self.__check_pv_name(pv_name):
            raise ValueError(
                "The PV name can only containes letter (upper or lower)"
                ", number ad the character ':'"
            )

        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")

        with self.__lock.gen_wlock():
            if pv_name in self.__monitor_pv_handler:
                logging.info(
                    "Monitor already activate for pv {}".format(pv_name))
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
        if not self.__check_pv_name(pv_name):
            raise ValueError(
                "The PV name can only containes letter (upper or lower)"
                ", number ad the character ':'"
            )

        with self.__lock.gen_wlock():
            if pv_name not in self.__monitor_pv_handler:
                logging.info(
                    "Monitor already stopped for pv {}".format(pv_name))
                return
            del self.__monitor_pv_handler[pv_name]
            self.__broker.remove_topic(self.__normalize_pv_name(pv_name))

        # send message to k2eg from activate (only for last topics) 
        # monitor(just in case it is not already activated)
        self.__broker.send_stop_monitor_command(
            pv_name,
            self.__normalize_pv_name(pv_name)
        )
