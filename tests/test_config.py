import os

from dynaconf import Dynaconf
from k2eg.k2eg import k2eg

def test_k2eg_config_with_env():
    try:
        # test env variable configuration
        os.environ["K2EG_KAFKA_BROKER_URL"] = 'localhost:1234'
        settings = Dynaconf(
            envvar_prefix="K2EG",
            settings_files=["settings.toml", ".secrets.toml"],
            ignore_unknown_envvars=True
        )
        assert settings.KAFKA_BROKER_URL == "localhost:1234"
    finally:
        del os.environ["K2EG_KAFKA_BROKER_URL"]
    
    
