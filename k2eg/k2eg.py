import logging
from dynaconf import Dynaconf
import threading
from readerwriterlock import RWLock
from kafka import KafkaConsumer
from kafka import KafkaProducer
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
        self.__producer = KafkaProducer(bootstrap_servers=self.settings.kafka_broker_url)
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
            

    def get(self, pv_name):
        logging.info("Get for pv {}".format(pv_name))
        return pv_name
    
    def monitor(self, pv_name, handler):
        """ Add a new monitor for pv if it is not already activated
        Parameters
                ----------
                pv_name : str
                    The name of the PV to monitor
                handler: function
                    The handler to be called when a message is received
        """
        with self.__lock.get_wlock():
            if self.__monitor_pv_handler.has_key(pv_name):
                logging.info("MOnitor already activate for pv {}".format(pv_name))
                return
            self.__monitor_pv_handler[pv_name] = handler
        
        
