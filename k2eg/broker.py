import datetime
import json
import os
import logging
import threading
import configparser
import time
from confluent_kafka import Consumer, TopicPartition, Producer, OFFSET_END, OFFSET_BEGINNING
from confluent_kafka import KafkaError, KafkaException

class TopicUnknown(Exception):
    """Exception raised when the timeout is expired on operation"""
    def __init__(self, message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class TopicChecker:
    def __init__(self):
        self.__topics_to_check = []
        self.__check_timeout = None
        self.__mutex = threading.Lock()

    def __check_for_topic(self, topic_name, consumer:Consumer):
        cluster_metadata = consumer.list_topics(topic_name, 1.0)
        return cluster_metadata.topics[topic_name].error \
                != KafkaError.UNKNOWN_TOPIC_OR_PART

    def add_topic(self, topic_name):
        with self.__mutex:
            logging.debug(f"Add topic {topic_name} to checker")
            self.__topics_to_check.append(topic_name)

    def remove_topic(self, topic_name):
        with self.__mutex:
            if topic_name in self.__topics_to_check:
                logging.debug(f"remove topic {topic_name} to checker")
                self.__topics_to_check.remove(topic_name)
            else:
                logging.debug(f"topic {topic_name} not in checker")

    def update_metadata(self, consumer: Consumer) -> bool:
        to_check =  self.__check_timeout is None or \
                    (self.__check_timeout is not None and \
                    datetime.datetime.now() > self.__check_timeout)
        if to_check:
            with self.__mutex:
                for t in self.__topics_to_check:
                    found = self.__check_for_topic(t, consumer)
                    logging.debug(f"Check for topic {t} => found:{found}")
                    # low, high = consumer.get_watermark_offsets(t)
                    # logging.debug(f'Found max and min [{high},{low}] index for topic: {t}')
                    if found:
                        logging.debug(f"Remove topic {t} from checker")
                        self.__topics_to_check.remove(t)

                if(len(self.__topics_to_check)>0):
                    self.__check_timeout = datetime.datetime.now() \
                        + datetime.timedelta(seconds=3)

class Broker:
    """
    Represent the abstraction of the protocol 
    which the k2eg client talk with the k2eg 
    gateway

    ...

    Attributes
    ----------

    Methods
    -------
    """
    def __init__(
        self, 
        environment_id: str,
        app_name:str,
        group_name: str = None,
        app_instance_unique_id:str = "1",
        startup_tout: int = 10):
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
        if group_name is None:
            group_name = app_name
        enviroment_set: str = 'DEFAULT'
        self.__initialized=False
        # Get the current directory of the script
        current_configuration_dir = os.getenv(
            'K2EG_PYTHON_CONFIGURATION_PATH_FOLDER', 
            os.path.dirname(os.path.realpath(__file__))+"/environment"
        ) 
        enable_kafka_debug = os.getenv(
            'K2EG_PYTHON_ENABLE_KAFKA_DEBUG_LOG', 
            'false'
        ).lower in ("yes", "true", "t", "1")

        self.__enviroment_set = enviroment_set
        # Create a new ConfigParser object
        self.__config = configparser.ConfigParser()

        # Read the configuration file
        self.__config.read(
            os.path.join(
            current_configuration_dir, 
            f'{environment_id}.ini')
            )

        self.__check_config(app_name)

        config_consumer = {
            'bootstrap.servers': self.__config.get(
                self.__enviroment_set, 'kafka_broker_url'
                ), 
            'group.id': group_name,
            'group.instance.id': app_name+'_'+app_instance_unique_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': 'true',
            'topic.metadata.refresh.interval.ms': '60000',
        }
        if enable_kafka_debug is True:
            config_consumer['debug'] = 'consumer,fetch'

        self.__consumer = Consumer(config_consumer)
        config_producer = {
            'bootstrap.servers': self.__config.get(
                self.__enviroment_set, 'kafka_broker_url'
                ),
            #'debug': 'consumer,cgrp,topic,fetch',
        }
        self.__producer = Producer(config_producer)
        self.__reply_topic = app_name + '-reply'
        self.__reply_partition_assigned = threading.Event()
        self.__subribed_topics = [self.__reply_topic]

        # start consuming from reply topic
        self.__consumer.subscribe(self.__subribed_topics, on_assign=self.__on_assign)
        self.__reply_topic_joined = False
        self.__topic_checker = TopicChecker()
        # wait for consumer join the partition
        self.wait_for_reply_available(startup_tout)
        

    def __manage_old_state(self):
        self.__consumer.poll(1.0)
        assigned_topics = self.__consumer.assignment()
        for t in assigned_topics:
            logging.debug(f'latest assigned topic: {t}')
        self.__consumer.unsubscribe()

    # point always to the end of the topic
    def __on_assign(self, consumer, partitions):
        for p in partitions:
            logging.debug(f"Joined topic {p.topic} with partition {p.partition}")
            if p.topic == self.__reply_topic:
                self.__reply_topic_joined = True
                self.__reply_partition_assigned.set()
                #if p.offset==-1 or p.offset==-1001:
                logging.debug(f'Force to reading from the end for topic: {p.topic}')
                #p.offset = OFFSET_END
            else:
                try:
                    low, high = consumer.get_watermark_offsets(p)
                    logging.debug(f'Found max and min [{high},{low}] index for topic: {p.topic}')
                    # in this case we have to go one index behing to start reading from the
                    # last element in the queue
                    if high >= 1:
                        # new_offset = high
                        logging.debug(f"offset for topic '{p.topic}' is '{high}'")
                        p.offset = OFFSET_END
                    elif high < 0:
                        p.offset = OFFSET_END
                    else:
                        p.offset = OFFSET_BEGINNING
                except Exception as e:
                    logging.debug(f'got exception on metadata refresh: {e}')
                    p.offset = OFFSET_END       
        consumer.assign(partitions)


    def __check_config(self, app_name):
        if not self.__config.has_option(self.__enviroment_set, 'kafka_broker_url'):
            raise ValueError('[kafka_broker_url] Kafka broker url is mandatory')
        if not self.__config.has_option(self.__enviroment_set, 'k2eg_cmd_topic'):
            raise ValueError("[k2eg_cmd_topic] The topic "
                             "for send command to k2eg is mandatory")

    def get_reply_topic(self):
        return self.__reply_topic

    def wait_for_reply_available(self, timeout):
        """ Wait untile the consumer has joined the reply topic
        """
        start_time = time.time()
        end_time = start_time + timeout
        while self.__reply_partition_assigned.wait(1) is False:
            if time.time() > end_time:
                raise TimeoutError("Function timed out")
            logging.debug("waiting for reply topic to join")
            self.__consumer.poll(1)

    def reset_reply_topic_to_ts(self, timestamp):
        self.reset_topic_offset_in_time(self.__reply_topic, timestamp)

    def reset_topic_offset_in_time(self, topic, timestamp):
        """ Set the topic offset to the specific timestamp
        """
        partitions = self.__consumer.list_topics(topic).topics[topic].partitions.keys()

        # Create TopicPartition objects for each partition, with the specific timestamp
        topic_partitions = [
            TopicPartition(topic, p, int(timestamp * 1000)) for p in partitions
        ]

        # Get the offsets for the specific timestamps
        try:
            offsets_for_times = self.__consumer.offsets_for_times(topic_partitions)
            # Set the starting offset of the consumer to the returned offsets
            for tp in offsets_for_times:
                if tp.offset != -1 or tp.offset != -1001:  # If an offset was found
                    self.__consumer.seek(tp)
                else:
                    tp.offset = OFFSET_BEGINNING
        except KafkaException as e:
            logging.error(e)
       
    def get_next_message(self, timeout = 0.300):
        message = self.__consumer.poll(timeout=timeout)
        # give a chanche to update metadata ofr pending topics
        self.__topic_checker.update_metadata(self.__consumer)
        if message is None:
            return None
        # tp = TopicPartition(message.topic(), message.partition(), message.offset() + 1)
        # # Asynchronous commit
        # self.__consumer.commit(offsets=[tp], asynchronous=True)
        if message.error():
            if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                logging.info(f"Topic {message.topic()} not found, add it to checker")
                self.__topic_checker.add_topic(message.topic())
        return message
    
    def commit_current_fetched_message(self):
        self.__consumer.commit()

    def add_topic(self, new_topic):
        if new_topic == self.__reply_topic:
            raise ValueError(
                f'The topic name {self.__reply_topic} cannot be used'
                )
        if new_topic not in self.__subribed_topics:
            logging.debug(f'Start consuming from topic: {new_topic}')
            self.__subribed_topics.append(new_topic)
            self.__consumer.subscribe(
                self.__subribed_topics, on_assign=self.__on_assign)
        else:
            logging.debug(f'Topic {new_topic} is already consuming')

    def remove_topic(self, topic_to_remove):
        if topic_to_remove == self.__reply_topic:
            raise ValueError(
                f'The topic name {self.__reply_topic} cannot be used'
                )
        self.__topic_checker.remove_topic(topic_to_remove)
        if topic_to_remove in self.__subribed_topics:
            logging.debug(f'Topic {topic_to_remove} will be removed')
            self.__subribed_topics.remove(topic_to_remove)
            self.__consumer.subscribe(
                self.__subribed_topics, on_assign=self.__on_assign
                )
        else:
            logging.debug(f'Topic {topic_to_remove} is not present')
  
    def send_command(self, message: str):
        broker_cmd_in_topic = self.__config.get(self.__enviroment_set, 'k2eg_cmd_topic')
        self.__producer.produce(
            broker_cmd_in_topic,
            message
        )
        self.__producer.flush()

    def send_get_command(self, pv_uri, reply_id):
        get_json_msg = {
            "command": "get",
            "serialization": "msgpack",
            "pv_name": pv_uri,
            "reply_topic": self.__reply_topic,
            "reply_id": reply_id
        }
        self.send_command(json.dumps(get_json_msg))

    def send_start_monitor_command(self, pv_uri, pv_reply_topic, reply_id):
        # send command
        monitor_json_msg = {
            "command": "monitor",
            "serialization": "msgpack",
            "pv_name": pv_uri,
            "activate": True,
            "reply_topic": self.__reply_topic,
            "reply_id": reply_id,
            "monitor_dest_topic": pv_reply_topic
        }
        self.send_command(json.dumps(monitor_json_msg))    
    
    def send_start_monitor_command_many(self, pv_uri_list, reply_id):
        # send command
        monitor_json_msg = {
            "command": "multi-monitor",
            "serialization": "msgpack",
            "activate": True,
            "pv_name_list": pv_uri_list,
            "reply_topic": self.__reply_topic,
            "reply_id": reply_id
        }
        self.send_command(json.dumps(monitor_json_msg))  

    def send_put_command(self, pv_uri: str, value: any, reply_id: str):
        put_value_json_msg = {
            "command": "put",
            "pv_name": pv_uri,
            "value": str(value),
            "reply_topic": self.__reply_topic,
            "reply_id": reply_id,
            "serialization": "msgpack"
        }
        self.send_command(json.dumps(put_value_json_msg))   

    def initialized(self):
        if not self.self.__initialized:
            raise 

    def close(self):
        self.__producer.flush()
        self.__consumer.close()