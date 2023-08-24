import pytest
from k2eg.broker import Broker

def test_broker_bad_conf():
    with pytest.raises(ValueError, 
                       match=r"[kafka_broker_url].*"):
                       Broker('bad_test')
    
def test_broker():
    try:
        Broker('test')
    except ValueError:
        pytest.fail("This should not be happen")