# K2EG Python Library

SImple Library for talk with k2eg gateway through kafka message buf

## Configuration
K2EG python lib uses the configparser package configurator. It need the following keys:

```
[DEFAULT]
kafka_broker_url=<kafka broker url>
k2eg_cmd_topic=<gateway command input topic>
reply_topic=<reply topic>
```

the class ***dml*** take in input the name of the environment to configure the broker. The name of the environment whill be used to select the ***.ini*** file for configure the broker class.

for example given the below folder:

```
conf-folder/
    env_1.ini
    env_2.ini
```

the below code snippet will take the configuration from the file env_1.ini
```python
from k2eg.dml import dml as k2eg

k = k2eg('env_1')
get_value = k.get('...', 'pva')
```

preconfigurated environment are stored into an internal folder.

### Custom configuration location
The ***K2EG_PYTHON_CONFIGURATION_PATH_FOLDER*** environment variable can be used for specify a custom configuraiton folder