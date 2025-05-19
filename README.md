# K2EG Python Library

K2EG (Kafka to EPICS Gateway) is a scalable, high-performance gateway that bridges EPICS (Experimental Physics and Industrial Control System) process variables (PVs) with modern data streaming platforms using Apache Kafka. It enables seamless integration between control systems and data processing pipelines, supporting both EPICS Channel Access (CA) and PVAccess (PVA) protocols. The K2EG Python Library provides a simple, high-level API for interacting with the K2EG gateway, allowing users to perform operations such as reading and writing PVs, monitoring real-time updates, and managing snapshots of PV states, all through Kafka-backed messaging. This library is ideal for scientific facilities, accelerators, and industrial automation environments that require robust, distributed control and data acquisition.

A key feature of K2EG is the **snapshot operation**. Unlike a simple get on multiple PVs, a snapshot is a coordinated data acquisition (DAQ) operation that captures the values of a set of PVs at the same instant. Snapshots can be triggered automatically at regular intervals (recurring snapshots) or manually by the developer using the provided API. This mechanism is designed for use cases where synchronized acquisition of multiple PVs is required, such as in experimental physics, diagnostics, or control system archiving.

## Configuration
K2EG python lib uses the configparser package configurator. It needs the following keys:

```
[DEFAULT]
kafka_broker_url=<kafka broker url>
k2eg_cmd_topic=<gateway command input topic>
reply_topic=<reply topic>
```

The class ***dml*** takes as input the name of the environment to configure the broker. The name of the environment will be used to select the ***.ini*** file for configuring the broker class.

For example, given the below folder:

```
conf-folder/
    env_1.ini
    env_2.ini
```

The below code snippet will take the configuration from the file env_1.ini:
```python
from k2eg.dml import dml as k2eg

k = k2eg('env_1', 'app-name')
got_value = k.get('pva://...')
```

Preconfigured environments are stored in an internal folder.

### Custom configuration location
The ***K2EG_PYTHON_CONFIGURATION_PATH_FOLDER*** environment variable can be used to specify a custom configuration folder.
***K2EG_CLI_DEFAULT_ENVIRONMENT*** => is the default environment for the K2EG demo CLI.
***K2EG_CLI_DEFAULT_APP_NAME*** => is the default app name to use for the demo CLI.

---

# k2eg.dml Python Client

This module provides a Python client for interacting with the K2EG system, supporting operations such as get, put, monitor, and snapshot on process variables (PVs) using Kafka as the backend.

## Class: `dml`

### Constructor

#### `__init__(environment_id: str, app_name: str, group_name: str = None)`
Initializes the K2EG client.

- **Parameters:**
  - `environment_id` (str): The environment identifier, is the name of the configuration file `.ini` found on the configuration folder pointed by the ***K2EG_PYTHON_CONFIGURATION_PATH_FOLDER*** variable.
  - `app_name` (str): The application name (mandatory), it define the name of the kafka topic to use for the reply messages from the k2eg broker.
  - `group_name` (str, optional): The group name for Kafka consumer group.

- **Raises:**  
  - `ValueError`: If `app_name` is not provided.

---

### Public Methods

#### `wait_for_backends()`
Waits for the Kafka reply topic to become available before proceeding. It is mandatory to wait that kafka connection and partion are associated to the client before staratin usding k2eg api.

---

#### `get(pv_url: str, timeout: float = None)`
Performs a synchronous get operation on a PV.

- **Parameters:**
  - `pv_url` (str): The PV URI (e.g., `pva://my:pv`).
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The value of the PV, or the raw result if not found.

- **Raises:**  
  - `ValueError`: If the protocol is not 'pva' or 'ca'.
  - `OperationTimeout`: If the operation times out.

---

#### `put(pv_url: str, value: any, timeout: float = None)`
Sets the value for a single PV.

- **Parameters:**
  - `pv_url` (str): The PV URI.
  - `value` (any): The value to set.
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The result of the put operation.

- **Raises:**  
  - `ValueError`: If the protocol is not 'pva' or 'ca'.
  - `OperationTimeout`: If the operation times out.

---

#### `monitor(pv_url: str, handler: Callable[[str, dict], None], timeout: float = None)`
Adds a monitor for a PV if not already activated.

- **Parameters:**
  - `pv_url` (str): The PV URI.
  - `handler` (Callable): Function to call when a message is received.
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The result of the monitor activation.

- **Raises:**  
  - `ValueError`: If the protocol is not 'pva' or 'ca'.
  - `OperationTimeout`: If the operation times out.

> **Note:** Monitors are automatically removed from the K2EG gateway when there are no consumers for a specific PV anymore (i.e., when no client is consuming from the corresponding Kafka topic). This ensures efficient resource usage on the gateway.

---

#### `monitor_many(pv_uri_list: list[str], handler: Callable[[str, dict], None], timeout: float = None)`
Adds monitors for multiple PVs.

- **Parameters:**
  - `pv_uri_list` (list[str]): List of PV URIs.
  - `handler` (Callable): Function to call when a message is received.
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The result of the monitor activation.

- **Raises:**  
  - `ValueError`: If any protocol is not 'pva' or 'ca'.
  - `OperationTimeout`: If the operation times out.

> **Note:** Monitors are automatically removed from the K2EG gateway when there are no consumers for a specific PV anymore (i.e., when no client is consuming from the corresponding Kafka topic). This ensures efficient resource usage on the gateway.
---

#### `stop_monitor(pv_name: str)`
Removes the monitor for a specific PV.

- **Parameters:**
  - `pv_name` (str): The name of the PV.

> **Note:** This method only stops the local K2EG Python library listener for PV events. On the K2EG gateway, the monitor will continue to be published as long as at least one K2EG client instance is consuming data from the specific monitored PV. The monitor is removed from the gateway only when no clients are consuming from the corresponding Kafka topic.

---

#### `snapshot(pv_uri_list: list[str], handler: Callable[[str, dict], None]) -> str`
Performs a snapshot creation for a list of PVs.

- **Parameters:**
  - `pv_uri_list` (list[str]): List of PV URIs.
  - `handler` (Callable): Function to call asynchronously with the snapshot results.

- **Returns:**  
  - The snapshot ID (str).

- **Raises:**  
  - `ValueError`: If any protocol is not 'pva' or 'ca'.

---

#### `snapshot_recurring(properties: SnapshotProperties, handler: Callable[[str, Snapshot], None], timeout: float = None)`
Creates a recurring snapshot for a list of PVs.

- **Parameters:**
  - `properties` (SnapshotProperties): Snapshot configuration.
  - `handler` (Callable): Function to call asynchronously with the snapshot results.
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The result of the snapshot creation.

- **Raises:**  
  - `ValueError`: If any protocol is not 'pva' or 'ca'.
  - `OperationTimeout`: If the operation times out.
  - `OperationError`: If the server returns an error.

---

#### `snapshost_trigger(snapshot_name: str, timeout: float = None)`
Triggers a new publishing of a specific recurring snapshot.

- **Parameters:**
  - `snapshot_name` (str): The name of the snapshot.
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The result of the trigger operation.

- **Raises:**  
  - `ValueError`: If the snapshot name is invalid.
  - `OperationTimeout`: If the operation times out.

---

#### `snapshot_stop(snapshot_name: str, timeout: float = None)`
Stops a recurring snapshot operation.

- **Parameters:**
  - `snapshot_name` (str): The name of the snapshot.
  - `timeout` (float, optional): Timeout in seconds.

- **Returns:**  
  - The result of the stop operation.

- **Raises:**  
  - `ValueError`: If the snapshot name is invalid.
  - `OperationTimeout`: If the operation times out.

---

#### `snapshot_sync(pv_uri_list: list[str], timeout: float = 10.0) -> list[dict[str, Any]]`
Performs a snapshot operation synchronously and returns the result.

- **Parameters:**
  - `pv_uri_list` (list[str]): List of PV URIs.
  - `timeout` (float, optional): Timeout in seconds (default: 10.0).

- **Returns:**  
  - A dictionary with keys `'error'` and `'snapshot'`.

- **Raises:**  
  - `OperationTimeout`: If the operation times out.

---

#### `close()`
Closes the client, terminating background threads and closing the broker connection.

---

## Exception Classes

- **OperationTimeout:** Raised when an operation times out.
- **OperationError:** Raised when an operation returns an error.

---

## Example Usage

```python
# filepath: /workspace/README.md
from k2eg.dml import dml
from k2eg.broker import SnapshotProperties

client = dml(environment_id="dev", app_name="myapp")

# Get a PV value
value = client.get("pva://my:pv", timeout=5.0)

# Put a PV value
client.put("pva://my:pv", 42, timeout=5.0)

# Monitor a PV
def handler(pv_name, data):
    print(f"{pv_name}: {data}")

client.monitor("pva://my:pv", handler, timeout=5.0)

# Recurring snapshot example
def snapshot_handler(snapshot_id, snapshot_obj):
    print(f"Snapshot {snapshot_id}: {snapshot_obj.results}")

properties = SnapshotProperties(
    snapshot_name="my_snapshot",
    pv_uri_list=["pva://my:pv1", "ca://my:pv2"],
    triggered = False
    # ...other SnapshotProperties fields as needed...
)
# Stop a recurring snapshot
client.snapshot_stop("my_snapshot", timeout=5.0)

# Recurring triggered snapshot example
def snapshot_handler(snapshot_id, snapshot_obj):
    print(f"Snapshot {snapshot_id}: {snapshot_obj.results}")

properties = SnapshotProperties(
    snapshot_name="my_snapshot",
    pv_uri_list=["pva://my:pv1", "ca://my:pv2"],
    triggered = True
    # ...other SnapshotProperties fields as needed...
)

# Trigger a recurring snapshot manually
client.snapshost_trigger("my_snapshot", timeout=5.0)

# Stop a recurring snapshot
client.snapshot_stop("my_snapshot", timeout=5.0)
```