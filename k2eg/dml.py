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

class dml:
    """K2EG client"""
    def __init__(self, environment_id:str):
        self.settings = Dynaconf(
            envvar_prefix="K2EG",
            settings_files=["settings.toml", ".secrets.toml"],
            ignore_unknown_envvars=True
        )
        self.__lock = rwlock.RWLockFairD()
        self.__reply_partition_assigned = threading.Event()
        config_consumer = {
            'bootstrap.servers': self.settings.kafka_broker_url,  # Change this to your broker(s)
            'group.id': self.settings.group_id, #+'_'+str(uuid.uuid1()),  # Change this to your group
            'auto.offset.reset': 'latest',
        }
        self.__consumer = Consumer(config_consumer)
        self.__consumer.subscribe([self.settings.reply_topic], on_assign=self.__on_assign)
        #reset to listen form now
        self.__reset_topic_offset_in_time(self.settings.reply_topic, int(time.time() * 1000))
        config_producer = {
            'bootstrap.servers': self.settings.kafka_broker_url,  # Change this to your broker(s)
        }
        self.__producer = Producer(config_producer)
        self.__consume_data = True
        self.__thread = threading.Thread(
            target=self.__consumer_handler
        )
        self.__thread.start()
        self.__monitor_pv_handler = {}
        self.reply_wait_condition = threading.Condition()
        self.reply_ready_event = threading.Event()
        self.reply_message = {}


    def __on_assign(self, consumer, partitions):
        logging.debug('Topic assignment:', partitions)
        self.__reply_partition_assigned.set()

    def __reset_topic_offset_in_time(self, topic, timestamp):
        """ Set the topic offset to the end of messages
        """
        # low, high = self.__consumer.get_watermark_offsets(TopicPartition('test', 0))
        # print("the latest offset is {}".format(high))
        # c.assign([TopicPartition('test', 0, high-1)])
        # Get the partitions for the topic
        partitions = self.__consumer.list_topics(topic).topics[topic].partitions.keys()

        # Create TopicPartition objects for each partition, with the specific timestamp
        topic_partitions = [TopicPartition(topic, p, int(timestamp * 1000)) for p in partitions]

        # Get the offsets for the specific timestamps
        offsets_for_times = self.__consumer.offsets_for_times(topic_partitions)

        # Set the starting offset of the consumer to the returned offsets
        for tp in offsets_for_times:
            if tp.offset != -1:  # If an offset was found
                self.__consumer.seek(tp)


    def __del__(self):
        # Perform cleanup operations when the instance is deleted
        self.close()

    def close(self):
        self.__consume_data = False
        self.__thread.join()
        self.__producer.flush()
        self.__consumer.close()

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
            message = self.__consumer.poll(timeout = 0.1)
            if message is None: continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.error('{} [{}]reached end at offset {}'.format(message.topic(), message.partition(), message.offset()))
                elif message.error():
                    logging.error(message.error())
            else:
                # good message
                if message.topic() == self.settings.reply_topic:
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
                self.__consumer.commit()

    def __check_pv_name(self, pv_name):
        pattern = r'^[a-zA-Z0-9:]+$'
        if re.match(pattern, pv_name):
            return True
        else:
            return False

    def __normalize_pv_name(self, pv_name):
        return pv_name.replace(":", "_")

    def wait_for_reply_available(self):
        """ Wait untile the consumer has joined the reply topic
        """
        self.__reply_partition_assigned.wait()

    def get(self, pv_name: str, protocol: str = 'pva'):
        """ Perform the get operation
        """
        if protocol.lower() != "pva" and protocol.lower() != "ca":
            raise ValueError("The portocol need to be one of 'pva'  'ca'")
        
        # wait for consumer joined the topic
        self.wait_for_reply_available()
        # clear the reply message for the requested pv
        self.reply_message[pv_name] = None
        fetched = False
        get_json_msg = {
            "command": "get",
            "serialization": "msgpack",
            "protocol": protocol.lower(),
            "pv_name": pv_name,
            "dest_topic": self.settings.reply_topic,
        }
        # send message to k2eg
        self.__producer.produce(
            self.settings.k2eg_cmd_topic,
            value=json.dumps(get_json_msg) 
        )
        self.__producer.flush()
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
            # subscribe to all needed topic
            # incllude the reply topic
            topics.append(self.settings.reply_topic)
            for pv in self.__monitor_pv_handler:
                # create topic name from the pv one
                topics.append(self.__normalize_pv_name(pv))
            logging.debug("start subscribtion on topics {}".format(topics))
            self.__consumer.subscribe(topics, on_assign=self.__on_assign)

        # send message to k2eg from activate (only for last topics) monitor(just in case it is not already activated)
        monitor_json_msg = {
            "command": "monitor",
            "serialization": "msgpack",
            "protocol": protocol.lower(),
            "pv_name": pv_name,
            "dest_topic": self.__normalize_pv_name(pv_name),
            "activate": True
        }
        # send message to k2eg
        self.__producer.produce(
            self.settings.k2eg_cmd_topic,
            value=json.dumps(monitor_json_msg)
        )
        self.__producer.flush()
    
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
            # subscribe to all needed topic
            # incllude the reply topic
            topics.append(self.settings.reply_topic)
            for pv in self.__monitor_pv_handler:
                # create topic name from the pv one
                topics.append(self.__normalize_pv_name(pv))
            logging.debug("start subscribtion on topics {}".format(topics))
            if topics.count != 0:
                self.__consumer.subscribe(topics, on_assign=self.__on_assign)
            else:
                self.__consumer.unsubscribe()

        # send message to k2eg from activate (only for last topics) monitor(just in case it is not already activated)
        stop_monitor_json_msg = {
            "command": "monitor",
            "serialization": "msgpack",
            "pv_name": pv_name,
            "dest_topic": self.__normalize_pv_name(pv_name),
            "activate": False
        }
        # send message to k2eg
        self.__producer.produce(
            self.settings.k2eg_cmd_topic,
            value=json.dumps(stop_monitor_json_msg)
        )
        self.__producer.flush()

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
        put_value_json_msg = {
            "command": "put",
            "protocol": protocol,
            "pv_name": pv_name,
            "value": str(value)
        }
        # send message to k2eg
        self.__producer.produce(
            self.settings.k2eg_cmd_topic,
            value=json.dumps(put_value_json_msg)
        )
        self.__producer.flush()
