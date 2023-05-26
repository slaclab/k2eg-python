"""Top-level package for k2eg library."""
from dynaconf import Dynaconf
import logging

class k2eg:
    def __init__(self):
        self.settings = Dynaconf(
                envvar_prefix="K2EG",
                settings_files=["settings.toml", ".secrets.toml"],
                ignore_unknown_envvars=True
            )

    def get(self, pv_name):
        logging.info("Get for pv {}".format(pv_name))
        return pv_name
