import os
from k2eg.k2eg import k2eg

def test_k2eg_config_with_env():
    try:
        # test env variable configuration
        os.environ["K2EG_KAFKA_BROKER_URL"] = 'localhost:1234'
        k = k2eg()
        assert k.settings.KAFKA_BROKER_URL == "localhost:1234"
    finally:
        del os.environ["K2EG_KAFKA_BROKER_URL"]
    
    
