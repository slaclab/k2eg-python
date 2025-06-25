from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Any, Dict
from uuid import uuid1
import k2eg
from k2eg.broker import SnapshotProperties, SnapshotType
from k2eg.dml import Snapshot, _filter_pv_uri
import time
import pytest
from unittest import TestCase
from k2eg.serialization import Scalar

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
    k = k2eg.dml('test', 'app-test', uuid1())
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
    get_value = k.get('pva://channel:ramp:ramp', timeout=2.0)
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
        k.monitor('pva://channel:ramp:rampa', monitor_handler)
        while received_message is None and retry < 20:
            retry = retry+1
            time.sleep(2)
    finally:
        k.stop_monitor("channel:ramp:rampa")
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
    k.put("pva://variable:a", Scalar(payload=0))
    k.put("pva://variable:b", Scalar(payload=0))
    time.sleep(1)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 0, "value should not be 0"
    k.put("pva://variable:a", Scalar(payload=2))
    k.put("pva://variable:b", Scalar(payload=2))
    #give some time to ioc to update
    time.sleep(1)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 4, "value should not be 0"

def test_multi_threading_put():
    put_dic={
        "pva://variable:a": Scalar(payload=0),
        "pva://variable:b": Scalar(payload=0)
    }
    with ThreadPoolExecutor(10) as executor:
        for key, value in put_dic.items():
            executor.submit(put, key, value)
    time.sleep(5)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 0, "value should not be 0"
    put_dic={
        "pva://variable:a": Scalar(payload=2),
        "pva://variable:b": Scalar(payload=2)
    }
    with ThreadPoolExecutor(10) as executor:
        for key, value in put_dic.items():
            executor.submit(put, key, value)
    #give some time to ioc to update
    time.sleep(5)
    res_get = k.get("pva://variable:sum")
    assert res_get['value'] == 4, "value should not be 0"

def put(key, value):
    try:
        k.put(key, value, 10)
        print(f"Put {key} with value {value}")
    except Exception as e:
        print(f"An error occured: {e}")
    

    
def test_put_timeout():
    with pytest.raises(k2eg.OperationTimeout, 
                    match=r"Timeout.*"):
                    k.put("pva://bad:pv:name", Scalar(0), timeout=0.5)

# def test_put_nttable():
#     nt_labels = [
#         "element", "device_name", "s", "z", "length", "p0c",
#         "alpha_x", "beta_x", "eta_x", "etap_x", "psi_x",
#         "alpha_y", "beta_y", "eta_y", "etap_y", "psi_y"
#     ]
#     table = NTTable(labels=nt_labels)

#     # 3) Add each column of data
#     table.set_column("element",["SOL9000", "XC99", "YC99"])
#     table.set_column("device_name",["SOL:IN20:111", "XCOR:IN20:112", "YCOR:IN20:113"])
#     table.set_column("s", [0.0, 0.0, 0.0])
#     table.set_column("z", [0.0, 0.0, 0.0])
#     table.set_column("length", [0.0, 0.0, 0.0])
#     table.set_column("p0c", [0.0, 0.0, 0.0])
#     table.set_column("alpha_x", [0.0, 0.0, 0.0])
#     table.set_column("beta_x", [0.0, 0.0, 0.0])
#     table.set_column("eta_x", [0.0, 0.0, 0.0])
#     table.set_column("etap_x", [0.0, 0.0, 0.0])
#     table.set_column("psi_x", [0.0, 0.0, 0.0])
#     table.set_column("alpha_y", [0.0, 0.0, 0.0])
#     table.set_column("beta_y", [0.0, 0.0, 0.0])
#     table.set_column("eta_y", [0.0, 0.0, 0.0])
#     table.set_column("etap_y", [0.0, 0.0, 0.0])
#     table.set_column("psi_y", [0.0, 0.0, 0.0])
#     t_dictionary = table.to_dict()
#     k.put("pva://K2EG:TEST:TWISS", table)
#     # get the value to check if all ahas been set
#     res_get = k.get("pva://K2EG:TEST:TWISS")
#     print(res_get)
#     assert res_get is not None, "value should not be None"
#     assert res_get['value'] is not None, "value should not be None"
#     assert res_get['value'] == t_dictionary['value'], "value should be the same as the one putted"
    
#     # now set the vallue to increment of 1 where possible
#     table.set_column("element",["1", "2", "3"])
#     table.set_column("device_name",["1", "2", "3"])
#     table.set_column("s", [1.0, 1.0, 1.0])
#     table.set_column("z", [1.0, 1.0, 1.0])
#     table.set_column("length", [1.0, 1.0, 1.0])
#     table.set_column("p0c", [1.0, 1.0, 1.0])
#     table.set_column("alpha_x", [1.0, 1.0, 1.0])
#     table.set_column("beta_x", [1.0, 1.0, 1.0])
#     table.set_column("eta_x", [1.0, 1.0, 1.0])
#     table.set_column("etap_x", [1.0, 1.0, 1.0])
#     table.set_column("psi_x", [1.0, 1.0, 1.0])
#     table.set_column("alpha_y", [1.0, 1.0, 1.0])
#     table.set_column("beta_y", [1.0, 1.0, 1.0])
#     table.set_column("eta_y", [1.0, 1.0, 1.0])
#     table.set_column("etap_y", [1.0, 1.0, 1.0])
#     table.set_column("psi_y", [1.0, 1.0, 1.0])
#     updated_t_dictionary = table.to_dict()
#     k.put("pva://K2EG:TEST:TWISS", table)
#     # get the value to check if all ahas been set
#     res_get = k.get("pva://K2EG:TEST:TWISS")
#     print(res_get)
#     assert res_get is not None, "value should not be None"
#     assert res_get['value'] is not None, "value should not be None"
#     assert res_get['value'] == updated_t_dictionary['value'], "value should be the same as the one putted"

def test_put_wrong_device_timeout():
    with pytest.raises(k2eg.OperationError):
                    k.put("pva://bad:pv:name", Scalar(payload=0))

def test_snapshot_on_simple_fixed_pv():
    retry = 0
    snapshot_id = None
    received_snapshot = None
    def snapshot_handler(id, snapshot_data):
        nonlocal snapshot_id
        nonlocal received_snapshot
        if snapshot_id == id:
            received_snapshot = snapshot_data

    try:
        snapshot_id = k.snapshot(['pva://variable:a', 'pva://variable:b'], snapshot_handler)
        while (received_snapshot is None ) and retry < 3:
            retry = retry+1
            time.sleep(2)
        # received_snapshot shuld be a dict with the snapshot data
        assert isinstance(received_snapshot, dict), "value should be a dict"
        assert 'variable:a' in received_snapshot, "value should not be None"
        assert 'variable:b' in received_snapshot, "value should not be None"
    except Exception as e:
        assert False, f"An error occured: {e}"

def test_snapshot_on_simple_fixed_pv_sync():
    try:
        received_snapshot = k.snapshot_sync(['pva://variable:a', 'pva://variable:b'])
        # received_snapshot shuld be a dict with the snapshot data
        assert isinstance(received_snapshot, dict), "value should be a dict"
        assert "error" in received_snapshot, "error should be in the snapshot"
        assert received_snapshot['error'] == 0, "error should be 0"
        assert 'variable:a' in received_snapshot, "value should not be None"
        assert 'variable:b' in received_snapshot, "value should not be None"
    except Exception as e:
        assert False, f"An error occured: {e}"
        
def test_recurring_snapshot():
    retry = 0
    snapshot_name = "snap_1"
    received_snapshot:list[Snapshot] = []
    def snapshot_handler(id, snapshot_data:Dict[str, Any]):
        nonlocal snapshot_name
        nonlocal received_snapshot
        if snapshot_name == id:
            received_snapshot.append(snapshot_data)
            
    try:
        result = k.snapshot_stop(snapshot_name)
        print(result)
        time.sleep(1)
        result = k.snapshot_recurring(
            SnapshotProperties(
                snapshot_name = snapshot_name,
                time_window = 1000,
                repeat_delay = 0,
                pv_uri_list = ['pva://variable:a', 'pva://variable:b'],
                triggered=False,
            ),
            handler=snapshot_handler,
            timeout=10,
        )
        print(result)
        while (len(received_snapshot) == 0 ) and retry < 5:
            retry = retry+1
            time.sleep(5)
        k.snapshot_stop(snapshot_name)
        time.sleep(1)
        # received_snapshot shuld be a dict with the snapshot data
        assert len(received_snapshot) > 0, "snapshot should not be None"
        #print a json description for the dictionary
        print(received_snapshot[0])
        assert received_snapshot[0]['header_timestamp'] > 0, "header_timestamp should be valid"
        assert received_snapshot[0]['tail_timestamp'] > 0, "tail_timestamp should be valid"
        assert received_snapshot[0]['iteration'] > 0, "interation should be valid"
        assert 'variable:a' in received_snapshot[0] and isinstance(received_snapshot[0]['variable:a'], list), "variable:a need be contained into the snapshot"
        assert 'variable:b' in received_snapshot[0] and isinstance(received_snapshot[1]['variable:a'], list), "variable:b need be contained into the snapshot"
    except Exception as e:
        assert False, f"An error occured: {e}"
    finally:
        k.snapshot_stop(snapshot_name)
        
        
def test_recurring_snapshot_check_for_empty_pv():
    retry = 0
    snapshot_name = "snap_1"
    received_snapshot:list[Snapshot] = []
    def snapshot_handler(id, snapshot_data:Dict[str, Any]):
        nonlocal snapshot_name
        nonlocal received_snapshot
        if snapshot_name == id:
            received_snapshot.append(snapshot_data)
            
    try:
        result = k.snapshot_stop(snapshot_name)
        print(result)
        time.sleep(1)
        result = k.snapshot_recurring(
            SnapshotProperties(
                snapshot_name = snapshot_name,
                time_window = 1000,
                repeat_delay = 0,
                pv_uri_list = ['pva://variable:a', 'pva://channel:ramp:ramp'],
                triggered=False,
                type=SnapshotType.TIMED_BUFFERED,
            ),
            handler=snapshot_handler,
            timeout=10,
        )
        print(result)
        while (len(received_snapshot) == 0 ) and retry < 5:
            retry = retry+1
            time.sleep(5)
        k.snapshot_stop(snapshot_name)
        time.sleep(1)
        # received_snapshot shuld be a dict with the snapshot data
        assert len(received_snapshot) > 0, "snapshot should not be None"
        #check that in every snapshot variable:a is present with a list of one value or zero
        for snapshot in received_snapshot:
            print(snapshot)
            assert 'variable:a' in snapshot, "variable:a should be present in the snapshot"
            assert isinstance(snapshot['variable:a'], list), "variable:a should be a list"
            assert len(snapshot['variable:a']) <= 1, "variable:a should be a list of one value or zero"
            #check that variable:b is not present in the snapshot
            assert 'channel:ramp:ramp' in snapshot, "channel:ramp:ramp should be present in the snapshot"
            assert isinstance(snapshot['channel:ramp:ramp'], list), "channel:ramp:ramp should be a list"
        # `
        
    except Exception as e:
        assert False, f"An error occured: {e}"
    finally:
        k.snapshot_stop(snapshot_name)
        time.sleep(1)

def test_recurring_snapshot_triggered():
    snapshot_name = "snap_1"
    received_snapshot:list[Snapshot] = []
    def snapshot_handler(id, snapshot_data:Snapshot):
        nonlocal snapshot_name
        nonlocal received_snapshot
        if snapshot_name == id:
            received_snapshot.append(snapshot_data)
            
    try:
        result = k.snapshot_stop(snapshot_name)
        print(result)
        time.sleep(1)
        result = k.snapshot_recurring(
            SnapshotProperties(
                snapshot_name = snapshot_name,
                time_window = 1000,
                repeat_delay = 0,
                pv_uri_list = ['pva://variable:a', 'pva://variable:b'],
                triggered=True,
            ),
            handler=snapshot_handler,
            timeout=10,
        )
        print(result)
        time.sleep(2)
        # send 2 snapshots
        result = k.snapshost_trigger(snapshot_name)
        print(result)
        time.sleep(2)
        result = k.snapshost_trigger(snapshot_name)
        print(result)
        time.sleep(2)
        k.snapshot_stop(snapshot_name)
        time.sleep(1)
        assert len(received_snapshot) == 2 , "snapshot should only one"
        assert received_snapshot[0]['header_timestamp'] > 0, "header_timestamp should be valid"
        assert received_snapshot[0]['tail_timestamp'] > 0, "tail_timestamp should be valid"
        assert received_snapshot[0]['iteration'] > 0, "interation should be valid"
        assert 'variable:a' in received_snapshot[0] and isinstance(received_snapshot[0]['variable:a'], list), "variable:a need be contained into the snapshot"
        assert 'variable:b' in received_snapshot[0] and isinstance(received_snapshot[1]['variable:a'], list), "variable:b need be contained into the snapshot"
    except Exception as e:
        assert False, f"An error occured: {e}"
    finally:
        k.snapshot_stop("snap_1")
        time.sleep(1)
        
def test_recurring_snapshot_timed_buffered():
    retry = 0
    snapshot_name = "snap_1"
    received_snapshot:list[Snapshot] = []
    def snapshot_handler(id, snapshot_data:Dict[str, Any]):
        nonlocal snapshot_name
        nonlocal received_snapshot
        if snapshot_name == id:
            received_snapshot.append(snapshot_data)
            
    try:
        result = k.snapshot_stop(snapshot_name)
        print(result)
        time.sleep(1)
        result = k.snapshot_recurring(
            SnapshotProperties(
                snapshot_name = snapshot_name,
                time_window = 5000,
                repeat_delay = 0,
                pv_uri_list = ['pva://channel:ramp:ramp', 'pva://channel:ramp:rampa'],
                triggered=False,
                type=SnapshotType.TIMED_BUFFERED,
                pv_field_filter_list = ['value', 'timeStamp'],
            ),
            handler=snapshot_handler,
            timeout=10,
        )
        print(result)
        while (len(received_snapshot) == 0 ) and retry < 5:
            retry = retry+1
            time.sleep(5)
        k.snapshot_stop(snapshot_name)
        time.sleep(1)
        # received_snapshot shuld be a dict with the snapshot data
        assert len(received_snapshot) > 0, "snapshot should not be None"
        #print a json description for the dictionary
        print(received_snapshot[0])
        assert received_snapshot[0]['header_timestamp'] > 0, "header_timestamp should be valid"
        assert received_snapshot[0]['tail_timestamp'] > 0, "tail_timestamp should be valid"
        assert received_snapshot[0]['iteration'] > 0, "interation should be valid"
        assert 'channel:ramp:ramp' in received_snapshot[0] and isinstance(received_snapshot[0]['channel:ramp:ramp'], list), "variable:a need be contained into the snapshot"
        assert 'channel:ramp:rampa' in received_snapshot[0] and isinstance(received_snapshot[0]['channel:ramp:rampa'], list), "variable:b need be contained into the snapshot"
    except Exception as e:
        assert False, f"An error occured: {e}"
    finally:
        k.snapshot_stop(snapshot_name)
        time.sleep(1)
        

def test_recurring_snapshot_time_buffered_with_sub_push():
    retry = 0
    snapshot_name = "snap_1"
    received_snapshot:list[Snapshot] = []
    def snapshot_handler(id, snapshot_data:Dict[str, Any]):
        nonlocal snapshot_name
        nonlocal received_snapshot
        if snapshot_name == id:
            received_snapshot.append(snapshot_data)
            
    try:
        result = k.snapshot_stop(snapshot_name)
        print(result)
        time.sleep(1)
        result = k.snapshot_recurring(
            SnapshotProperties(
                snapshot_name = snapshot_name,
                time_window = 4000,
                repeat_delay = 0,
                sub_push_delay_msec = 1000,
                pv_uri_list = ['pva://channel:ramp:ramp'],
                triggered=False,
                type=SnapshotType.TIMED_BUFFERED,
                pv_field_filter_list = ['value'],
            ),
            handler=snapshot_handler,
            timeout=10,
        )
        print(result)
        while (len(received_snapshot) == 0 ) and retry < 5:
            retry = retry+1
            time.sleep(5)
        k.snapshot_stop(snapshot_name)
        time.sleep(1)
        # received_snapshot shuld be a dict with the snapshot data
        assert len(received_snapshot) > 0, "snapshot should not be None"
        #print a json description for the dictionary
        print(received_snapshot[0])
        assert received_snapshot[0]['header_timestamp'] > 0, "header_timestamp should be valid"
        assert received_snapshot[0]['tail_timestamp'] > 0, "tail_timestamp should be valid"
        assert received_snapshot[0]['iteration'] > 0, "interation should be valid"
        assert 'channel:ramp:ramp' in received_snapshot[0] and isinstance(received_snapshot[0]['channel:ramp:ramp'], list), "variable:a need be contained into the snapshot"
        # channel:ramp:ramp need to have four values
        assert len(received_snapshot[0]['channel:ramp:ramp']) >= 4, "channel:ramp:ramp should have four values"
    except Exception as e:
        assert False, f"An error occured: {e}"
    finally:
        k.snapshot_stop(snapshot_name)
        time.sleep(1)