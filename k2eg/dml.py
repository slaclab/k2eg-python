import logging
import time
from dynaconf import Dynaconf
import threading
from readerwriterlock import rwlock
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka import Producer
from confluent_kafka import KafkaError
import re
import json
import msgpack
from typing import Callable

from .broker import Broker

class dml:
    """K2EG client"""
    def __init__(self, environment_id:str):
        self.__broker = Broker(environment_id)
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

    def __from_json(self, j_msg):
        print('__from_json')

    def __from_msgpack(self, m_msg):
        pv_name = None
        converted_msg = None
        decodec_msg = msgpack.loads(m_msg)
        if isinstance(decodec_msg, dict) and len(decodec_msg)==1:
            pv_name = list(decodec_msg.keys())[0]
            converted_msg = decodec_msg
        return pv_name, converted_msg

    def __from_msgpack_compack(self, mc_msg):
        print('__from_msgpack_compack')

    def __decode_message(self, msg):
        """ Decode single message
        """
        pv_name = None
        converted_msg = None
        headers = msg.headers()
        if headers is None:
            logging.error("Message without header received")
            return pv_name, converted_msg
        
        for key, value in headers:
            if key == 'k2eg-ser-type':
                st = value.decode('utf-8')
                if st == "json":
                    pv_name, converted_msg = self.__from_json(msg.value())
                elif st == "msgpack":
                    pv_name, converted_msg = self.__from_msgpack(msg.value())
                elif st == "msgpack-compact":
                    pv_name, converted_msg = self.__from_msgpack_compack(msg.value())
                break   
        return pv_name, converted_msg

    def __process_message(self, pv_name, converted_msg):
        """ Process single message
        """
        with self.__lock.gen_rlock():
            if pv_name not in self.__monitor_pv_handler:
                return
            self.__monitor_pv_handler[pv_name](converted_msg)
            logging.debug('read message sent to {} hanlder'.format(self.__monitor_pv_handler[pv_name]))

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
            if message is None: continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.error('{} [{}]reached end at offset {}'.format(message.topic(), message.partition(), message.offset()))
                elif message.error():
                    logging.error(message.error())
            else:
                # good message
                if message.topic() == self.__broker.get_reply_topic():
                    logging.info("received reply with offset {}".format(message.offset))
                    pv_name, converted_msg = self.__decode_message(message)
                    if pv_name == None or converted_msg == None:
                        continue
                    with self.reply_wait_condition:
                        self.reply_message[pv_name] = converted_msg
                        self.reply_wait_condition.notifyAll()
                else:
                    logging.info("received monitor message with offset {} from topic {}".format(message.offset, message.topic))
                    pv_name, converted_msg = self.__decode_message(message)
                    if pv_name == None or converted_msg == None:
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

    def get(self, pv_name: str, protocol: str = 'pva'):
        """ Perform the get operation
        """
        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        
        # wait for consumer joined the topic
        self.__broker.wait_for_reply_available()
        # clear the reply message for the requested pv
        self.reply_message[pv_name] = None
        fetched = False
        # send message to k2eg
        self.__broker.send_get_command(
            pv_name,
            protocol.lower()
        )
        # wait for response
        while(not fetched):
            logging.info("Wait for message")
            with self.reply_wait_condition:
                self.reply_wait_condition.wait()
                if self.reply_message[pv_name] == None:
                    continue
                fetched = True
        return self.reply_message[pv_name][pv_name]
                


    def monitor(self, pv_name: str, handler: Callable[[any], None], protocol: str = 'pva'):
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
                "The PV name can only containes letter (upper or lower), number ad the character ':'")

        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")

        topics = []
        with self.__lock.gen_wlock():
            if pv_name in self.__monitor_pv_handler:
                logging.info(
                    "Monitor already activate for pv {}".format(pv_name))
                return
            self.__monitor_pv_handler[pv_name] = handler
            self.__broker.add_topic(self.__normalize_pv_name(pv_name))

        # send message to k2eg from activate (only for last topics) monitor(just in case it is not already activated)
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
                "The PV name can only containes letter (upper or lower), number ad the character ':'")

        topics = []
        with self.__lock.gen_wlock():
            if pv_name not in self.__monitor_pv_handler:
                logging.info(
                    "Monitor already stopped for pv {}".format(pv_name))
                return
            del self.__monitor_pv_handler[pv_name]
            self.__broker.remove_topic(self.__normalize_pv_name(pv_name))

        # send message to k2eg from activate (only for last topics) monitor(just in case it is not already activated)
        self.__broker.send_stop_monitor_command(
            pv_name,
            self.__normalize_pv_name(pv_name)
        )

    def put(self, pv_name: str, value: any, protocol: str = 'pva'):
        """ Set the value for a single pv

        Args:
            pv_name (str): is the name of the pv
            value (str): is the new value
            protocol (str): the protocl of the pv, the default is pva
        Raises:
            ValueError: if some paramter are not valid
        """
        if not self.__check_pv_name(pv_name):
            raise ValueError(
                "The PV name can only containes letter (upper or lower), number ad the character ':'")

        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        
        # create emssage for k2eg
        self.__broker.send_put_command(
            pv_name,
            value,
            protocol
        )
