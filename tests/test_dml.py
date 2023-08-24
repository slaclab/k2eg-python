import k2eg
import time
import pytest

def test_k2eg_get():
    k = k2eg.dml('test', 'app-test')
    try:
        get_value = k.get('channel:ramp:ramp', 'pva')
        assert get_value is not None, "value should not be None"
    finally:
        if k is not None: k.close()

def test_k2eg_get_timeout():
    k = k2eg.dml('test', 'app-test')
    try:
         with pytest.raises(k2eg.OperationTimeout, 
                       match=r"Timeout.*"):
                       k.get('bad:pv:name', 'pva', timeout=0.5)
    finally:
        if k is not None: k.close()

def test_k2eg_get_bad_pv():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(k2eg.OperationError):
                        k.get('bad:pv:name', 'pva')
    finally:
        if k is not None: k.close()

def test_k2eg_get_default_protocol():
    k = k2eg.dml('test', 'app-test')
    try:
        get_value = k.get('channel:ramp:ramp')
        assert get_value is not None, "value should not be None"
    finally:
        if k is not None: k.close()

def test_k2eg_monitor():
    k = k2eg.dml('test', 'app-test')
    try:
        received_message = None

        def monitor_handler(new_value):
            nonlocal received_message
            received_message = new_value

        k.monitor('channel:ramp:ramp', monitor_handler, 'pva')
        while received_message is None:
            time.sleep(2)
        
        assert received_message is not None, "value should not be None"
    finally:
        if k is not None: 
            k.stop_monitor('channel:ramp:ramp')
            k.close()

def test_put():
    k = k2eg.dml('test', 'app-test')
    try:
        k.put("variable:a", 0)
        k.put("variable:b", 0)
        time.sleep(1)
        res_get = k.get("variable:sum")
        assert res_get['value'] == 0, "value should not be 0"
        k.put("variable:a", 2)
        k.put("variable:b", 2)
        #give some time to ioc to update
        time.sleep(1)
        res_get = k.get("variable:sum")
        assert res_get['value'] == 4, "value should not be 0"
    finally:
        k.close()

def test_put_timeout():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(k2eg.OperationTimeout, 
                        match=r"Timeout.*"):
                        k.put("bad:pv:name", 0, timeout=0.5)
    finally:
        k.close()

def test_put_wrong_device_timeout():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(k2eg.OperationError):
                        k.put("bad:pv:name", 0)
    finally:
        k.close()
