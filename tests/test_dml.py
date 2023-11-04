import logging
import k2eg
from k2eg.dml import _filter_pv_uri
import time
import pytest
from unittest import TestCase

k: k2eg.dml = None
TestCase.maxDiff = None

@pytest.fixture(scope="session", autouse=True)
def my_setup(request):
    print('Enable k2eg')
    global k
    logging.basicConfig(
            format="[%(asctime)s %(levelname)-8s] %(message)s",
            level=logging.DEBUG,
        )
    k = k2eg.dml('test', 'app-test')
    def fin():
        global k
        if k is not None:
              k.close()
    request.addfinalizer(fin)

def test_uri_protocol_ioc_attribute():
    protocol, resource = _filter_pv_uri('pva://ioa:a.attribute')
    assert protocol == 'pva', "protocol should not be None"
    assert resource == 'ioa:a.attribute', "protocol should not be None"

def test_uri_protocol_ioc_multilevel_attribute():
    protocol, resource = _filter_pv_uri('pva://ioa:a.attribute.sub-attribute')
    assert protocol == 'pva', "protocol should not be None"
    assert resource == 'ioa:a.attribute.sub-attribute', "protocol should not be None"

def test_uri_protocol_ioc():
    protocol, resource = _filter_pv_uri('pva://ioa:a')
    assert protocol == 'pva', "protocol should not be None"
    assert resource == 'ioa:a', "protocol should not be None"

def test_uri_protocol_ioc_uppercase_and_dash():
    protocol, resource = _filter_pv_uri('ca://VGXX-L3B-1602-PLOG')
    assert protocol == 'ca', "protocol should not be None"
    assert resource == 'VGXX-L3B-1602-PLOG', "protocol should not be None"


def test_exception_on_get_without_protocol():
    with pytest.raises(ValueError, 
                    match=r".*url.*not.*formed"):
                    k.get('//bad:pv:name', timeout=0.5)


def test_exception_on_get_with_bad_pv_name():
    with pytest.raises(ValueError, 
                    match=r".*url.*not.*formed"):
                    k.get('pva://', timeout=0.5)

def test_exception_on_get_with_bad_protocol():
    with pytest.raises(ValueError, 
                    match=r".*url.*not.*formed"):
                    k.get('unkonwn://', timeout=0.5)

def test_k2eg_get():
    get_value = k.get('pva://channel:ramp:ramp')
    assert get_value is not None, "value should not be None"

def test_k2eg_get_timeout():
    with pytest.raises(k2eg.OperationTimeout, 
                match=r"Timeout.*"):
                k.get('pva://bad:pv:name', timeout=0.5)

def test_k2eg_get_bad_pv():
    with pytest.raises(k2eg.OperationError):
                    k.get('pva://bad:pv:name')

def test_k2eg_monitor():
    retry = 0
    received_message = None

    def monitor_handler(pv_name, new_value):
        nonlocal received_message
        received_message = new_value
    try:
        k.monitor('pva://channel:ramp:ramp', monitor_handler)
        while received_message is None and retry < 20:
            retry = retry+1
            time.sleep(2)
    finally:
        k.stop_monitor("channel:ramp:ramp")
    assert received_message is not None, "value should not be None"

# def test_k2eg_monitor_on_already_started_mon():
#     last_received_data = None
#     previous_event_data = None
#     def monitor_handler(pv_name, new_value):
#         nonlocal last_received_data
#         logging.info(f"Received event from {pv_name}")
#         last_received_data = new_value
#     try:
#         retry = 0
#         #this will emit only one message
#         k.monitor('pva://variable:a', monitor_handler)
#         while last_received_data is None and retry < 50:
#             retry = retry+1
#             time.sleep(2)
        
#         assert last_received_data is not None, "value should not be None"
#         # now stop the consume
#         k.stop_monitor('variable:a')
#         logging.info("retry to reread form the same pv should receive the same last record")
#         previous_event_data = last_received_data
#         #reset variable for receive data
#         last_received_data = None
#         retry = 0
#         k.monitor('pva://variable:a', monitor_handler)
#         while last_received_data is None and retry < 50:
#             retry = retry+1
#             time.sleep(2)
#     finally:
#         k.stop_monitor("variable:a")
#     # now the two variable previous_event and last_received_method
#     # should be the same
#     assert (json.dumps(last_received_data) == json.dumps(previous_event_data)), "Dictionary need to be the same"

def test_k2eg_monitor_wrong():
    retry = 0
    received_message = None

    def monitor_handler(pv_name, new_value):
        nonlocal received_message
        received_message = new_value
    try:
        k.monitor('pva://channel:ramp:rampc', monitor_handler)
        while received_message is None and retry < 5:
            retry = retry+1
            time.sleep(2)
    finally:
        k.stop_monitor("channel:ramp:rampc")
    assert received_message is  None, "value be None"


def test_k2eg_monitor_many():
    retry = 0
    received_message_a = False
    received_message_b = False
    def monitor_handler(pv_name, new_value):
        nonlocal received_message_a
        nonlocal received_message_b
        if pv_name=='channel:ramp:rampa':
            received_message_a = True
        if pv_name=='channel:ramp:rampb':
            received_message_b = True
    try:
        k.monitor_many(['pva://channel:ramp:rampa', 'pva://channel:ramp:rampb'], monitor_handler)
        while (received_message_a is False or received_message_b is False) and retry < 20:
            retry = retry+1
            time.sleep(2)
    finally:
        k.stop_monitor("channel:ramp:rampa")
        k.stop_monitor("channel:ramp:rampb")
    assert received_message_a is not False, "value should not be None"
    assert received_message_b is not False, "value should not be None"

def test_put():
    k.put("pva://variable:a", 0)
    k.put("pva://variable:b", 0)
    time.sleep(1)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 0, "value should not be 0"
    k.put("pva://variable:a", 2)
    k.put("pva://variable:b", 2)
    #give some time to ioc to update
    time.sleep(1)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 4, "value should not be 0"

def test_put_timeout():
    with pytest.raises(k2eg.OperationTimeout, 
                    match=r"Timeout.*"):
                    k.put("pva://bad:pv:name", 0, timeout=0.5)


def test_put_wrong_device_timeout():
    with pytest.raises(k2eg.OperationError):
                    k.put("pva://bad:pv:name", 0)
