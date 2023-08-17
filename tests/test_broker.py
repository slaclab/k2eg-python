import time
import pytest
from k2eg.broker import Broker as broker
from confluent_kafka.admin import AdminClient, NewTopic

def test_broker_bad_conf():
    with pytest.raises(ValueError, 
                       match=r"[kafka_broker_url].*"):
                       broker('bad_test')
    
def test_broker():
    try:
        broker('test')
    except ValueError:
        pytest.fail("This should not be happen")