import pytest
from k2eg.broker import Broker

   
def test_broker():
    try:
        Broker('test')
    except ValueError:
        pytest.fail("This should not be happen")