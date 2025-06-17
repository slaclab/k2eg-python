import msgpack
from k2eg.serialization import Scalar, Vector, Generic, NTTable

def test_scalar():
    s = Scalar(payload=23.5)
    packed_s = s.to_msgpack()  
    data = msgpack.unpackb(packed_s, raw=False) 
    # check that i got this msgpack structure {'value': 23.5}
    print(f"Packed Scalar: {data}")   
    assert data == {'value': 23.5}

def test_vector():
    v = Vector(payload=[23.5, 24.0, 22.8])
    packed_v = v.to_msgpack()  
    data = msgpack.unpackb(packed_v, raw=False) 
    # check that i got this msgpack structure {'temperature': [23.5, 24.0, 22.8]}
    print(f"Packed Vector: {data}")   
    assert data == {'value': [23.5, 24.0, 22.8]}
    
def test_generic():
    g = Generic(payload={'key1': 'value1', 'key2': 42})
    packed_g = g.to_msgpack()  
    data = msgpack.unpackb(packed_g, raw=False) 
    # check that i got this msgpack structure {'value': {'key1': 'value1', 'key2': 42}}
    print(f"Packed Generic: {data}")   
    assert data == {'value': {'key1': 'value1', 'key2': 42}}

def test_nttable():
    ntt = NTTable(
        labels=["station", "anomaly_state"],
        payload=[{'station': 'station_1', 'anomaly_state': True}, {'station': 'station_2', 'anomaly_state': False}]
    )
    packed_ntt = ntt.to_msgpack()  
    data = msgpack.unpackb(packed_ntt, raw=False) 
    # check that i got this msgpack structure
    print(f"Packed NTTable: {data}")   
    assert data == {
        'value': [{'station': 'station_1', 'anomaly_state': True}, {'station': 'station_2', 'anomaly_state': False}]
    }