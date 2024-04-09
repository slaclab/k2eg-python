from concurrent.futures import ThreadPoolExecutor
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

def test_multi_threading_put():
    put_dic={
        "pva://variable:a": 0,
        "pva://variable:b": 0
    }
    with ThreadPoolExecutor(10) as executor:
        for key, value in put_dic.items():
            executor.submit(put, key, value)
    time.sleep(5)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 0, "value should not be 0"
    put_dic={
        "pva://variable:a": 2,
        "pva://variable:b": 2
    }
    with ThreadPoolExecutor(10) as executor:
        for key, value in put_dic.items():
            executor.submit(put, key, value)
    #give some time to ioc to update
    time.sleep(5)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 4, "value should not be 0"

# def test_multiple_put():
#     def monitor_handler(pv_name, new_value):
#        pass
        
#     monitor_pv = [
#                     'ca://SOLN:IN20:121:BACT',
#                     'ca://QUAD:IN20:121:BACT',
#                     'ca://QUAD:IN20:122:BACT',
#                     'ca://ACCL:IN20:300:L0A_PDES',
#                     'ca://ACCL:IN20:400:L0B_PDES',
#                     'ca://ACCL:IN20:300:L0A_ADES',
#                     'ca://ACCL:IN20:400:L0B_ADES',
#                     'ca://QUAD:IN20:361:BACT',
#                     'ca://QUAD:IN20:371:BACT',
#                     'ca://QUAD:IN20:425:BACT',
#                     'ca://QUAD:IN20:441:BACT',
#                     'ca://QUAD:IN20:511:BACT',
#                     'ca://QUAD:IN20:525:BACT',
#                     'ca://FBCK:BCI0:1:CHRG_S',
#                     'ca://CAMR:IN20:186:XRMS',
#                     'ca://CAMR:IN20:186:YRMS'
#                     ]
#     #k.monitor_many(monitor_pv, monitor_handler)
#     monitor_put = {
#         'pva://LUME:MLFLOW:SIGMA_X': '-99830.6330126242',
#         'pva://LUME:MLFLOW:SIGMA_Y': '225322.341204345',
#         'pva://LUME:MLFLOW:SIGMA_Z': '10352.2788770893'
#     }
#     time_start = time.time()
#     with ThreadPoolExecutor(10) as executor:
#         for key, value in monitor_put.items():
#             executor.submit(put, key, value)
#     time_end = time.time()
#     print(f"Time taken to put: {time_end - time_start} - {(time_end - time_start)/len(monitor_put) })")

def put(key, value):
    try:
        k.put(key, value, 10)
        print(f"Put {key} with value {value}")
    except Exception as e:
        print(f"An error occured: {e}")

def test_put_timeout():
    with pytest.raises(k2eg.OperationTimeout, 
                    match=r"Timeout.*"):
                    k.put("pva://bad:pv:name", 0, timeout=0.5)


def test_put_wrong_device_timeout():
    with pytest.raises(k2eg.OperationError):
                    k.put("pva://bad:pv:name", 0)
