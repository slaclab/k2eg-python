from k2eg.k2eg import k2eg

def test_k2eg_get():
    k = k2eg()
    assert k.monitor("channel:ramp:ramp") == "pv_name"