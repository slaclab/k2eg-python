from k2eg.k2eg import k2eg

def test_k2eg_get():
    k = k2eg()
    get_value = k.get('channel:ramp:ramp','pva')
    assert get_value != None, "value should not ne None"