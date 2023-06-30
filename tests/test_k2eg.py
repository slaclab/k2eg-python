from k2eg.dml import dml as k2eg
import time

def test_k2eg_get():
    k = k2eg('test')
    get_value = k.get('channel:ramp:ramp', 'pva')
    assert get_value != None, "value should not be None"
    k.close()

def test_k2eg_get_default_protocol():
    k = k2eg('test')
    get_value = k.get('channel:ramp:ramp')
    assert get_value != None, "value should not be None"
    k.close()

def test_k2eg_monitor():
    k = k2eg('test')
    received_message = None

    def monitor_handler(new_value):
        nonlocal received_message
        received_message = new_value

    k.monitor('channel:ramp:ramp', monitor_handler, 'pva')
    while received_message is None:
        time.sleep(2)
    
    assert received_message != None, "value should not be None"
    k.stop_monitor('channel:ramp:ramp')
    k.close()

def test_put():
    k = k2eg('test')
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
    
