import os
import uuid
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
        }
        self.__consumer = Consumer(config_consumer)
        config_producer = {
            'bootstrap.servers': self.__config.get(self.__enviroment_set, 'kafka_broker_url'),  # Change this to your broker(s)
        }
        self.__producer = Producer(config_producer)

    def __check_config(self):
        if not self.__config.has_option(self.__enviroment_set, 'kafka_broker_url'):
            raise ValueError('[kafka_broker_url] Kafka broker url is mandatory')
        if not self.__config.has_option(self.__enviroment_set, 'k2eg_cmd_topic'):
            raise ValueError('[k2eg_cmd_topic] The topic for send command to k2eg is mandatory')
        if not self.__config.has_option(self.__enviroment_set, 'reply_topic'):
            raise ValueError('[reply_topic] The reply topic for get answer from k2eg is mandatory')
        
    def get_reply_topic(self):
        return self.__config.get(self.__enviroment_set, 'reply_topic')
    
    def send_message_to_broker(self, message: str):
        broker_cmd_in_topic = self.__config.get('k2eg_cmd_topic')
        self.__producer.produce(
            broker_cmd_in_topic,
            message
        )
        self.__producer.flush()

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
    