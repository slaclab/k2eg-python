from time import sleep
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
import sys


def create_topics(admin, topics):
    new_topics = []
    for topic in topics:
        new_topics.append(NewTopic(
            topic, 
            num_partitions=1, 
            replication_factor=1))
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic {topic} already exists")
            else:
                sys.exit(1)

def delete_topics(a, topics):
    """ delete topics """
    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))

if __name__ == "__main__":
    admin = AdminClient({'bootstrap.servers': 'kafka:9092','debug': 'cgrp,topic,fetch'})
    print('delete topic')
    delete_topics(admin, ['cmd-in-topic', 'app-test-reply', 'ke2g-app-reply', 'channel_ramp_ramp', 'channel_ramp_rampa' 'channel_ramp_rampb', 'channel_ramp_rampc'])
    sleep(5)
    print('recreate topic')
    create_topics(admin, ['cmd-in-topic', 'app-test-reply', 'ke2g-app-reply', 'channel_ramp_ramp', 'channel_ramp_rampa' 'channel_ramp_rampb', 'channel_ramp_rampc'])