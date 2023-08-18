from k2eg.broker import Broker
from k2eg.dml import dml
import time
import pytest

def test_k2eg_get():
    b = Broker('test')
    k = dml(b, 'app-test')
    get_value = k.get('channel:ramp:ramp', 'pva')
    assert get_value is not None, "value should not be None"
    k.close()
    b.close()

def test_k2eg_get_timeout():
    b = Broker('test')
    k = dml(b, 'app-test')
    with pytest.raises(dml.OperationTimeout, 
                       match=r"Timeout.*"):
                       k.get('bad:pv:name', 'pva', timeout=0.5)
    k.close()
    b.close()

def test_k2eg_get_bad_pv():
    b = Broker('test')
    k = dml(b, 'app-test')
    with pytest.raises(dml.OperationError):
                       k.get('bad:pv:name', 'pva')
    k.close()
    b.close()

def test_k2eg_get_default_protocol():
    b = Broker('test')
    k = dml(b, 'app-test')
    get_value = k.get('channel:ramp:ramp')
    assert get_value is not None, "value should not be None"
    k.close()
    b.close()

def test_k2eg_monitor():
    b = Broker('test')
    k = dml(b, 'app-test')
    received_message = None

    def monitor_handler(new_value):
        nonlocal received_message
        received_message = new_value

    k.monitor('channel:ramp:ramp', monitor_handler, 'pva')
    while received_message is None:
        time.sleep(2)
    
    assert received_message is not None, "value should not be None"
    k.stop_monitor('channel:ramp:ramp')
    k.close()
    b.close()

def test_put():
    b = Broker('test')
    k = dml(b, 'app-test')
    try:
        res_put = k.put("variable:a", 0)
        assert res_put[0] == 0, "put should succeed"
        res_put = k.put("variable:b", 0)
        assert res_put[0] == 0, "put should succeed"
        time.sleep(1)
        res_get = k.get("variable:sum")
        assert res_get['value'] == 0, "value should not be 0"
        res_put = k.put("variable:a", 2)
        assert res_put[0] == 0, "put should succeed"
        res_put = k.put("variable:b", 2)
        assert res_put[0] == 0, "put should succeed"
        #give some time to ioc to update
        time.sleep(1)
        res_get = k.get("variable:sum")
        assert res_get['value'] == 4, "value should not be 0"
    finally:
        k.close()
        b.close()

def test_put_timeout():
    b = Broker('test')
    k = dml(b, 'app-test')
    with pytest.raises(dml.OperationTimeout, 
                       match=r"Timeout.*"):
                       k.put("bad:pv:name", 0, timeout=0.5)
    k.close()
    b.close()

def test_put_wrong_device_timeout():
    b = Broker('test')
    k = dml(b, 'app-test')
    with pytest.raises(dml.OperationError):
                       k.put("bad:pv:name", 0)
    k.close()
    b.close() 
