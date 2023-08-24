from token import EQUAL
import k2eg
from k2eg.dml import _filter_pv_uri
import time
import pytest

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

def test_exception_on_get_without_protocol():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(ValueError, 
                       match=r".*url.*not.*formed"):
                       k.get('//bad:pv:name', timeout=0.5)
    finally:
        if k is not None: k.close()

def test_exception_on_get_with_bad_pv_name():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(ValueError, 
                       match=r".*url.*not.*formed"):
                       k.get('pva://', timeout=0.5)
    finally:
        if k is not None: k.close()

def test_exception_on_get_with_bad_protocol():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(ValueError, 
                       match=r".*url.*not.*formed"):
                       k.get('unkonwn://', timeout=0.5)
    finally:
        if k is not None: k.close()

def test_k2eg_get():
    k = k2eg.dml('test', 'app-test')
    try:
        get_value = k.get('pva://channel:ramp:ramp')
        assert get_value is not None, "value should not be None"
    finally:
        if k is not None: k.close()

def test_k2eg_get_timeout():
    k = k2eg.dml('test', 'app-test')
    try:
         with pytest.raises(k2eg.OperationTimeout, 
                       match=r"Timeout.*"):
                       k.get('pva://bad:pv:name', timeout=0.5)
    finally:
        if k is not None: k.close()

def test_k2eg_get_bad_pv():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(k2eg.OperationError):
                        k.get('pva://bad:pv:name')
    finally:
        if k is not None: k.close()

def test_k2eg_monitor():
    k = k2eg.dml('test', 'app-test')
    try:
        received_message = None

        def monitor_handler(pv_name, new_value):
            nonlocal received_message
            received_message = new_value

        k.monitor('pva://channel:ramp:ramp', monitor_handler)
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
    finally:
        k.close()

def test_put_timeout():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(k2eg.OperationTimeout, 
                        match=r"Timeout.*"):
                        k.put("pva://bad:pv:name", 0, timeout=0.5)
    finally:
        k.close()

def test_put_wrong_device_timeout():
    k = k2eg.dml('test', 'app-test')
    try:
        with pytest.raises(k2eg.OperationError):
                        k.put("pva://bad:pv:name", 0)
    finally:
        k.close()