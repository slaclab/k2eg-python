import logging
from dynaconf import Dynaconf
import threading
from readerwriterlock import rwlock
from kafka import KafkaConsumer
from kafka import KafkaProducer
import re
import json
import uuid
import msgpack
import queue

class k2eg:
    """K2EG client"""

    def __init__(self):
        self.settings = Dynaconf(
            envvar_prefix="K2EG",
            settings_files=["settings.toml", ".secrets.toml"],
            ignore_unknown_envvars=True
        )
        self.__lock = rwlock.RWLockFairD()
        self.__consumer = KafkaConsumer(
            bootstrap_servers=self.settings.kafka_broker_url,
            group_id='k2eg_'+str(uuid.uuid1()),
            auto_offset_reset="latest")
        self.__consumer.subscribe([self.settings.reply_topic])
        self.__producer = KafkaProducer(
            bootstrap_servers=self.settings.kafka_broker_url,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.__thread = threading.Thread(
            target=self.__consumer_handler)
        self.__thread.start()
        self.__consume_data = True
        self.__monitor_pv_handler = {}
        self.reply_wait_condition = threading.Condition()
        self.reply_ready_event = threading.Event()
        self.reply_message = {}
    
    def __decode_message(self, msg):
        """ Decode single message
        """
        h = msg.headers
        if h and h.has_key('k2eg-ser-type'):
            st = h.get('k2eg-ser-type')
            if st == "json":
                pv_name, converted_msg = json.load(msg)
            elif st == "msgpack":
                pv_name, converted_msg = msgpack.loads(msg)
            elif st == "msgpack-compact":
                pv_name, converted_msg = msgpack.loads(msg)
            else:
                pv_name = None
                converted_msg = None
        return pv_name, converted_msg

    def __process_message(self, pv_name, converted_msg):
        """ Process single message
        """
        with self.__lock.get_rlock():
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
        for msg in self.__consumer:
            pv_name, converted_msg = self.__decode_message(msg)
            if pv_name != None and converted_msg != None:
                continue
            #check if message came from reply topic
            if msg.topic == self.settings.reply_topic:
                with self.reply_wait_condition:
                    self.reply_message[pv_name] = converted_msg
                    self.reply_wait_condition.notifyAll()
            else:
                self.__process_message(pv_name, converted_msg)

    def __check_pv_name(pv_name):
        pattern = r'^[a-zA-Z0-9:]+$'
        if re.match(pattern, pv_name):
            return True
        else:
            return False

    def __normalize_pv_name(pv_name):
        return pv_name.replace(":", "_")

    def get(self, pv_name, protocol):
        """ Perform the get operation
        """
        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        # clear the reply message for the requested pv
        self.reply_message[pv_name] = None
        fetched = False
        monitor_json_msg = {
            "command": "get",
            # "serialization": "msgpack",
            # "protocol": protocol.lower(),
            # "pv_name": pv_name,
            # "dest_topic": self.settings.reply_topic,
        }
        # send message to k2eg
        self.__producer.send(
            self.settings.k2eg_cmd_topic,
            value=monitor_json_msg
        )
        self.__producer.flush()
        # wait for response
        while(not fetched):
            with self.reply_wait_condition:
                self.reply_wait_condition.wait()
                if self.reply_message[pv_name] == None:
                    continue
                fetched = True
        return self.reply_message[pv_name]
                


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
            raise ValueError(
                "The PV name can only containes letter (upper or lower), number ad the character ':'")

        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")

        topics = []
        with self.__lock.get_wlock():
            if self.__monitor_pv_handler.has_key(pv_name):
                logging.info(
                    "MOnitor already activate for pv {}".format(pv_name))
                return
            self.__monitor_pv_handler[pv_name] = handler
            # subscribe to all needed topic
            # incllude the reply topic
            topics.append(self.settings.reply_topic)
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
