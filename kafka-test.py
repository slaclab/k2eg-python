import datetime
from confluent_kafka import Consumer, KafkaError
import confluent_kafka

topics_to_check = []
check_timeout = None

def my_assign (consumer, partitions):
    for p in partitions:
        low, high = consumer.get_watermark_offsets(p)
        p.offset = confluent_kafka.OFFSET_END
        print("offset=",p.offset)
    print('assign', partitions)
    print('position:',consumer.position(partitions))
    consumer.assign(partitions)

def check_for_topic(topic):
    cluster_metadata = consumer.list_topics(topic, 1.0)
    return cluster_metadata.topics[topic].error != KafkaError.UNKNOWN_TOPIC_OR_PART



def scan_topic_metadata():
    global check_timeout
    to_check =  check_timeout is None or \
                (check_timeout is not None and \
                datetime.datetime.now() > check_timeout)
    
    if to_check:
        for t in topics_to_check:
            print(f"Check for topic {t}:", end="")
            found = check_for_topic(t)
            if found:
                print("found")
                topics_to_check.remove(t)
            else:
                print('Topic found')
        if(len(topics_to_check)>0):
            check_timeout = datetime.datetime.now() + datetime.timedelta(seconds=3)


config_consumer = {
            'bootstrap.servers': '172.24.5.187:9094', 
            'group.id': 'group-id', #str(uuid.uuid1())[:8],
            'group.instance.id':'new-group-1',
            'auto.offset.reset': 'latest',
            'enable.auto.commit':'false',
            #'debug': 'consumer,cgrp,topic',
        }
consumer = Consumer(config_consumer)
consumer.subscribe(['very-new-topic'], on_assign=my_assign)
#reset_topic_offset_in_time('app-ml2-reply', time.time())

# Get the current timestamp

while(True):
    message = consumer.poll(0.3)
    scan_topic_metadata()
    if message is None:
        continue
    if message.error():
        if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            topics_to_check.append(message.topic())
            continue

    print(message.value()) 


