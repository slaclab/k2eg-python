import logging
from dynaconf import Dynaconf
import threading
from readerwriterlock import RWLock
from kafka import KafkaConsumer
from kafka import KafkaProducer
import re
import json

class k2eg:
    """K2EG client"""
    def __init__(self):
        self.settings = Dynaconf(
                envvar_prefix="K2EG",
                settings_files=["settings.toml", ".secrets.toml"],
                ignore_unknown_envvars=True
            )
        self.__lock = RWLock()
        self.__consumer = KafkaConsumer(bootstrap_servers=self.settings.kafka_broker_url)
        self.__producer = KafkaProducer(
            bootstrap_servers=self.settings.kafka_broker_url,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.__thread = threading.Thread(target=self.__consumer_handler, args=(1,))
        self.__thread.start()
        self.__consume_data = True
        self.__monitor_pv_handler = {}

    def __from_json(self, msg):
        print("__from_json")

    def __from_msgpack(self, msg):
        print("__from_msgpack")

    def __from_msgpack_compact(self, msg):
        print("__from_msgpack_compact")

    def __process_message(self, msg):
        """ Process single message
        """
        h = msg.headers
        if h and h.has_key('k2eg-ser-type'):
            st = h.get('k2eg-ser-type')
            if st == "json":
                pv_name, converted_msg = self.__from_json(msg)
            elif st == "json":
                pv_name, converted_msg = self.__from_msgpack(msg)
            else:
                pv_name, converted_msg = self.__from_msgpack_compact(msg)

            with self.__lock.get_rlock():
                logging.debug('read message with ser type {} and send to hanlder'.format(st, self.__monitor_pv_handler[pv_name]))
                self.__monitor_pv_handler[pv_name](converted_msg)
        else:
            print('message with no header')

    def __consumer_handler(self):
        """ Consume message form kafka consumer
        after the message has been consumed the header 'k2eg-ser-type' is checked 
        for find the serializaiton:
            json, 
            msgpack, 
            msgpack-compact
        """
        for msg in self.__consumer:
           self.__process_message(msg)
            
    def __check_pv_name(pv_name):
        pattern = r'^[a-zA-Z0-9:]+$'
        if re.match(pattern, pv_name):
            return True
        else:
            return False

    def __normalize_pv_name(pv_name):
        return pv_name.replace(":", "_")

    def get(self, pv_name):
        logging.info("Get for pv {}".format(pv_name))
        return pv_name
    
    def monitor(self, pv_name, protocol, handler):
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
        if not self.____check_pv_name(pv_name):
            raise ValueError("The PV name can only containes letter (upper or lower), number ad the character ':'")
        
        if protocol.lower() != "pva" and protocol.lower() != "ca":
             raise ValueError("The portocol need to be one of 'pva'  'ca'")
        
        topics = []
        with self.__lock.get_wlock():
            if self.__monitor_pv_handler.has_key(pv_name):
                logging.info("MOnitor already activate for pv {}".format(pv_name))
                return
            self.__monitor_pv_handler[pv_name] = handler
            #subscribe to all needed topic
            for pv in self.__monitor_pv_handler:
                # create topic name from the pv one
                topics.append(self.__normalize_pv_name(pv))
            logging.debug("start subscribtion on topics {}".format(topics))
            self.__consumer.subscribe(topics)

        # send message to k2eg from activate (only for last topics) monitor(just in case it is not already activated)
        monitor_json_msg = {
            "command": "monitor",
            "serialization": "msgpack",
            "protocol": protocol.lower(),
            "pv_name": pv_name,
            "dest_topic": self.__normalize_pv_name(pv),
            "activate": True
        }
        # send message to k2eg
        self.__producer.send(
            self.settings.k2eg_cmd_topic, 
            value=monitor_json_msg
            )
        self.__producer.flush()
        return True
        
        
