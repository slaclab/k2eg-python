from k2eg.k2eg import k2eg
import time

def test_k2eg_get():
    k = k2eg()
    k.wait_for_reply_available()
    get_value = k.get('channel:ramp:ramp', 'pva')
    assert get_value != None, "value should not be None"
    k.close()

def test_k2eg_get_default_protocol():
    k = k2eg()
    k.wait_for_reply_available()
    get_value = k.get('channel:ramp:ramp')
    assert get_value != None, "value should not be None"
    k.close()

def test_k2eg_monitor():
    k = k2eg()
    received_message = None

    def monitor_handler(new_value):
        nonlocal received_message
        received_message = new_value

    get_value = k.monitor('channel:ramp:ramp', monitor_handler, 'pva')
    while received_message is None:
        time.sleep(1)
    
    assert received_message != None, "value should not be None"
    k.stop_monitor('channel:ramp:ramp')
    k.close()