import json
import os
import uuid
import logging
import threading
import configparser
from confluent_kafka import Consumer, TopicPartition, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

class Broker:
    """
    Represent the abstraction of the protocol 
    which the k2eg client talk with the k2eg g
    ateway

    ...

    Attributes
    ----------

    Methods
    -------
    get_reply_topic()
        Return the string of the reply topic
    """
    def __init__(
        self, 
        environment_id: str, 
        group_name: str =  str(uuid.uuid1()),
        app_name:str =  str(uuid.uuid1())):
        """
        Parameters
        ----------
        environment_id : str
            The name of the environment to use, this wil be the danem of the
            ini file where to load the configuration. (<environment_id>.ini)
        group_name : str
            Is the group name to distribuite the data from different same 
            client instance
        """
        enviroment_set: str = 'DEFAULT'
        self.__initialized=False
        # Get the current directory of the script
        current_configuration_dir = os.getenv(
            'K2EG_PYTHON_CONFIGURATION_PATH_FOLDER', 
            os.path.dirname(os.path.realpath(__file__))
        ) 
        self.__enviroment_set = enviroment_set
        # Create a new ConfigParser object
        self.__config = configparser.ConfigParser()

        # Read the configuration file
        self.__config.read(
            os.path.join(
            current_configuration_dir, 
            f'environment/{environment_id}.ini')
            )

        self.__check_config(app_name)

        config_consumer = {
            'bootstrap.servers': self.__config.get(
                self.__enviroment_set, 'kafka_broker_url'
                ), 
            'group.id': group_name,
            'auto.offset.reset': 'latest',
            #'debug': 'consumer,cgrp,topic,fetch',
        }
        self.__consumer = Consumer(config_consumer)
        config_producer = {
            'bootstrap.servers': self.__config.get(
                self.__enviroment_set, 'kafka_broker_url'
                ),
            #'debug': 'consumer,cgrp,topic,fetch',
        }
        self.__producer = Producer(config_producer)
        self.__admin = AdminClient({'bootstrap.servers': self.__config.get(
                self.__enviroment_set, 'kafka_broker_url'
                )})
        self.__reply_topic = app_name + '-reply'
        self.__create_topics(self.__reply_topic)
        self.__reply_partition_assigned = threading.Event()
        self.__subribed_topics = [self.__reply_topic]
        self.__consumer.subscribe(self.__subribed_topics, on_assign=self.__on_assign)
        self.__initialized=True

    def __create_topics(self, topic_name: str):
        new_topics = [NewTopic(
            topic_name, 
            num_partitions=1, 
            replication_factor=1)]
        fs = self.__admin.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.debug(f"Topic {topic} created")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logging.debug(f"Topic {topic} already exists")
                else:
                    logging.FATAL(f"Failed to create topic {topic}: {e}")

    def __on_assign(self, consumer, partitions):
        logging.debug(f"Joined partition {partitions}")
        self.__reply_partition_assigned.set()

    def __check_config(self, app_name):
        if not self.__config.has_option(self.__enviroment_set, 'kafka_broker_url'):
            raise ValueError('[kafka_broker_url] Kafka broker url is mandatory')
        if not self.__config.has_option(self.__enviroment_set, 'k2eg_cmd_topic'):
            raise ValueError("[k2eg_cmd_topic] The topic "
                             "for send command to k2eg is mandatory")
        # if not self.__config.has_option(self.__enviroment_set, 'reply_topic'):
        #     raise ValueError("[reply_topic] The reply topic for "
        #                      "get answer from k2eg is mandatory")
        
    def get_reply_topic(self):
        return self.__reply_topic

    def wait_for_reply_available(self):
        """ Wait untile the consumer has joined the reply topic
        """
        self.__reply_partition_assigned.wait()

    def reset_reply_topic_to_ts(self, timestamp):
        self.reset_topic_offset_in_time(self.__reply_topic, timestamp)

    def reset_topic_offset_in_time(self, topic, timestamp):
        """ Set the topic offset to the end of messages
        """
        partitions = self.__consumer.list_topics(topic).topics[topic].partitions.keys()

        # Create TopicPartition objects for each partition, with the specific timestamp
        topic_partitions = [
            TopicPartition(topic, p, int(timestamp * 1000)) for p in partitions
            ]

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
            raise ValueError(
                f'The topic name {self.__reply_topic} cannot be used'
                )
        if new_topic not in self.__subribed_topics:
            self.__subribed_topics.append(new_topic)
            self.__consumer.subscribe(
                self.__subribed_topics, on_assign=self.__on_assign)

    def remove_topic(self, topic_to_remove):
        if topic_to_remove == self.__reply_topic:
            raise ValueError(
                f'The topic name {self.__reply_topic} cannot be used'
                )
        if topic_to_remove in self.__subribed_topics:
            self.__subribed_topics.remove(topic_to_remove)
            self.__consumer.subscribe(
                self.__subribed_topics, on_assign=self.__on_assign
                )
  
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
        # ensure topic exists
        self.__create_topics(pv_reply_topic)

        # send command
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

    def initialized(self):
        if not self.self.__initialized:
            raise 

    def close(self):
        self.__producer.flush()
        self.__consumer.close()