import json
import os
import uuid
import time
import logging
import threading
import configparser
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka import Producer
from confluent_kafka import KafkaError

class Broker:
    def __init__(self, environment_id: str, enviroment_set: str = 'DEFAULT'):
        # Get the current directory of the script
        current_dir = os.path.dirname(os.path.realpath(__file__))
        self.__enviroment_set = enviroment_set
        # Create a new ConfigParser object
        self.__config = configparser.ConfigParser()

        # Read the configuration file
        self.__config.read(os.path.join(current_dir, 'environment/{}.ini'.format(environment_id)))

        self.__check_config()

        config_consumer = {
            'bootstrap.servers': self.__config.get(self.__enviroment_set, 'kafka_broker_url'),  # Change this to your broker(s)
            'group.id': str(uuid.uuid1()),  # Change this to your group
            'auto.offset.reset': 'latest',
            #'debug': 'consumer,cgrp,topic,fetch',
        }
        self.__consumer = Consumer(config_consumer)
        config_producer = {
            'bootstrap.servers': self.__config.get(self.__enviroment_set, 'kafka_broker_url'),  # Change this to your broker(s)
            #'debug': 'consumer,cgrp,topic,fetch',
        }
        self.__producer = Producer(config_producer)
        self.__reply_topic = self.__config.get(self.__enviroment_set, 'reply_topic')
        self.__reply_partition_assigned = threading.Event()
        self.__subribed_topics = [self.__reply_topic]
        self.__consumer.subscribe(self.__subribed_topics, on_assign=self.__on_assign)
        self.reset_topic_offset_in_time(self.__reply_topic, int(time.time() * 1000))

    def __on_assign(self, consumer, partitions):
        logging.debug("Joined partition {}".format(partitions))
        self.__reply_partition_assigned.set()

    def __check_config(self):
        if not self.__config.has_option(self.__enviroment_set, 'kafka_broker_url'):
            raise ValueError('[kafka_broker_url] Kafka broker url is mandatory')
        if not self.__config.has_option(self.__enviroment_set, 'k2eg_cmd_topic'):
            raise ValueError('[k2eg_cmd_topic] The topic for send command to k2eg is mandatory')
        if not self.__config.has_option(self.__enviroment_set, 'reply_topic'):
            raise ValueError('[reply_topic] The reply topic for get answer from k2eg is mandatory')
        
    def get_reply_topic(self):
        return self.__config.get(self.__enviroment_set, 'reply_topic')

    def wait_for_reply_available(self):
        """ Wait untile the consumer has joined the reply topic
        """
        self.__reply_partition_assigned.wait()

    def reset_topic_offset_in_time(self, topic, timestamp):
        """ Set the topic offset to the end of messages
        """
        partitions = self.__consumer.list_topics(topic).topics[topic].partitions.keys()

        # Create TopicPartition objects for each partition, with the specific timestamp
        topic_partitions = [TopicPartition(topic, p, int(timestamp * 1000)) for p in partitions]

        # Get the offsets for the specific timestamps
        offsets_for_times = self.__consumer.offsets_for_times(topic_partitions)

        # Set the starting offset of the consumer to the returned offsets
        for tp in offsets_for_times:
            if tp.offset != -1:  # If an offset was found
                self.__consumer.seek(tp)
    
    def get_next_message(self, timeout = 0.1):
        return self.__consumer.poll(timeout)
    
    def commit_current_fetched_message(self):
        self.__consumer.commit()

    def add_topic(self, new_topic):
        if new_topic == self.__reply_topic:
            raise ValueError('The topic name {} cannot be used'.format(self.__reply_topic))
        if new_topic not in self.__subribed_topics:
            self.__subribed_topics.append(new_topic)
            self.__consumer.subscribe(self.__subribed_topics, on_assign=self.__on_assign)

    def remove_topic(self, topic_to_remove):
        if topic_to_remove == self.__reply_topic:
            raise ValueError('The topic name {} cannot be used'.format(self.__reply_topic))
        if topic_to_remove in self.__subribed_topics:
            self.__subribed_topics.remove(topic_to_remove)
            self.__consumer.subscribe(self.__subribed_topics, on_assign=self.__on_assign)
  
    def send_command(self, message: str):
        broker_cmd_in_topic = self.__config.get(self.__enviroment_set, 'k2eg_cmd_topic')
        self.__producer.produce(
            broker_cmd_in_topic,
            message
        )
        self.__producer.flush()

    def send_get_command(self, pv_name, protocol, reply_id):
        get_json_msg = {
            "command": "get",
            "serialization": "msgpack",
            "protocol": protocol.lower(),
            "pv_name": pv_name,
            "dest_topic": self.__reply_topic,
            "reply_id": reply_id
        }
        self.send_command(json.dumps(get_json_msg))

    def send_start_monitor_command(self, pv_name, protocol, pv_reply_topic, ):
        monitor_json_msg = {
            "command": "monitor",
            "serialization": "msgpack",
            "protocol": protocol.lower(),
            "pv_name": pv_name,
            "dest_topic": pv_reply_topic,
            "activate": True
        }
        self.send_command(json.dumps(monitor_json_msg))    

    def send_stop_monitor_command(self, pv_name, pv_reply_topic):
        monitor_json_msg = {
            "command": "monitor",
            "serialization": "msgpack",
            "pv_name": pv_name,
            "dest_topic": pv_reply_topic,
            "activate": False
        }
        self.send_command(json.dumps(monitor_json_msg))
    
    def send_put_command(self, pv_name: str, value: any, protocol:str, reply_id: str):
        put_value_json_msg = {
            "command": "put",
            "protocol": protocol,
            "pv_name": pv_name,
            "value": str(value),
            "reply_id": reply_id,
            "serialization": "msgpack",
            "dest_topic": self.__reply_topic,
        }
        self.send_command(json.dumps(put_value_json_msg))   

    def close(self):
        self.__producer.flush()
        self.__consumer.close()