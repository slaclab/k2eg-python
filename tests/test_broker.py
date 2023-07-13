import time
import pytest
from k2eg.broker import Broker as broker
from confluent_kafka.admin import AdminClient, NewTopic


@pytest.fixture(scope='session', autouse=True)
def my_fixture():
    print("Prepare kafka topics for test.")
    # Setup code goes here
    admin_client = AdminClient({
        "bootstrap.servers": "kafka:9092"
    })

    topic_list = [
           NewTopic("reply-topic-xyz", num_partitions=1, replication_factor=1),
           NewTopic("cmd-in-topic", num_partitions=1, replication_factor=1),
           ]
    admin_client.create_topics(topic_list)
    # Wait until the topic appears in the cluster (with a timeout)
    for i in range(10):
        cluster_topics = admin_client.list_topics().topics
        if "reply-topic-xyz" in cluster_topics and  "cmd-in-topic" in cluster_topics:
            print("Topics has been created.")
            break
        time.sleep(1)
    else:
        print("Topic was not created within the timeout period.")

def test_broker_bad_conf():
    with pytest.raises(ValueError, 
                       match=r"[kafka_broker_url].*"):
                       broker('bad_test')
    
def test_broker():
    try:
        broker('test')
    except ValueError:
        pytest.fail("This should not be happen")