import pytest
from k2eg.broker import Broker as broker

def test_broker_bad_conf():
    with pytest.raises(ValueError, 
                       match=r"[kafka_broker_url].*"):
                       b = broker('bad_test')
    
def test_broker():
    try:
         b = broker('test')
    except:
        pytest.fail("This should nto be happen")