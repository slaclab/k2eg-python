# K2EG Python Library

K2EG (Kafka to EPICS Gateway) is a scalable, high-performance gateway that bridges EPICS (Experimental Physics and Industrial Control System) process variables (PVs) with modern data streaming platforms using Apache Kafka. It enables seamless integration between control systems and data processing pipelines, supporting both EPICS Channel Access (CA) and PVAccess (PVA) protocols.

The K2EG Python Library provides a simple, high-level API for interacting with the K2EG gateway, allowing users to perform operations such as reading and writing PVs, monitoring real-time updates, and managing snapshots of PV states, all through Kafka-backed messaging. This library is ideal for scientific facilities, accelerators, and industrial automation environments that require robust, distributed control and data acquisition.

A key feature of K2EG is the **snapshot operation**. Unlike a simple get on multiple PVs, a snapshot is a coordinated data acquisition (DAQ) operation that captures the values of a set of PVs at the same instant. Snapshots can be triggered automatically at regular intervals (recurring snapshots) or manually by the developer using the provided API. This mechanism is designed for use cases where synchronized acquisition of multiple PVs is required, such as in experimental physics, diagnostics, or control system archiving.

---

## Configuration

K2EG Python lib uses the `configparser` package for configuration. It needs the following keys in the configuration file:

```ini
[DEFAULT]
kafka_broker_url=<kafka broker url>
k2eg_cmd_topic=<gateway command input topic>
reply_topic=<reply topic>
```

The class `dml` takes as input the name of the environment to configure the broker. The name of the environment will be used to select the `.ini` file for configuring the broker class.

**Example folder structure:**
```
conf-folder/
    env_1.ini
    env_2.ini
```

**Example usage:**
```python
from k2eg.dml import dml as k2eg

k = k2eg('env_1', 'app-name')
got_value = k.get('pva://...')
```

Preconfigured environments are stored in an internal folder.

### Custom Configuration Location

- `K2EG_PYTHON_CONFIGURATION_PATH_FOLDER`: Specify a custom configuration folder.
- `K2EG_CLI_DEFAULT_ENVIRONMENT`: Default environment for the K2EG demo CLI.
- `K2EG_CLI_DEFAULT_APP_NAME`: Default app name for the demo CLI.

---

# k2eg.dml Python Client

This module provides a Python client for interacting with the K2EG system, supporting operations such as get, put, monitor, and snapshot on process variables (PVs) using Kafka as the backend.

## Class: `dml`

### Constructor

#### `__init__(environment_id: str, app_name: str, group_name: str = None)`

Initializes the K2EG client.

- **Parameters:**
  - `environment_id` (str): The environment identifier (name of the `.ini` file in the configuration folder).
  - `app_name` (str): The application name (mandatory, defines the Kafka topic for reply messages).
  - `group_name` (str, optional): The group name for Kafka consumer group.

- **Raises:**  
  - `ValueError`: If `app_name` is not provided.

---

## Public Methods

### Backend and PV Operations

#### `wait_for_backends()`
Waits for the Kafka reply topic to become available before proceeding.

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

#### `put(pv_url: str, value: MessagePackSerializable, timeout: float = None)`
Sets the value for a single PV, supporting advanced types such as scalars, vectors, and structured tables.

- **Parameters:**
  - `pv_url` (`str`): The PV URI (e.g., `"pva://my:pv"`).
  - `value` (`MessagePackSerializable`):  
    An instance of a class derived from `MessagePackSerializable` (such as `Scalar`, `Vector`, `NTTable`, or `Generic`).  
    This allows you to send not only simple values, but also complex data structures (e.g., arrays or tables) to the PV.  
    The object will be automatically serialized to MessagePack before being sent.
  - `timeout` (`float`, optional): Timeout in seconds for the operation.

- **Returns:**  
  - The result of the put operation (typically a confirmation or status object).

- **Raises:**  
  - `ValueError`: If the protocol is not 'pva' or 'ca'.
  - `OperationTimeout`: If the operation times out.

**Usage Example:**
```python
from k2eg.serialization import Scalar, Vector, NTTable

# Scalar value
client.put("pva://my:pv", Scalar("value", 42))

# Vector value
client.put("pva://my:array", Vector([1, 2, 3, 4])) #the default key='value' is implicit

# NTTable value
 nt_labels = [
        "element", "device_name", "s", "z", "length", "p0c",
        "alpha_x", "beta_x", "eta_x", "etap_x", "psi_x",
        "alpha_y", "beta_y", "eta_y", "etap_y", "psi_y"
    ]
table = NTTable(labels=nt_labels)

# 3) Add each column of data
table.set_column("element",["SOL9000", "XC99", "YC99"])
table.set_column("device_name",["SOL:IN20:111", "XCOR:IN20:112", "YCOR:IN20:113"])
table.set_column("s", [0.0, 0.0, 0.0])
table.set_column("z", [0.0, 0.0, 0.0])
table.set_column("length", [0.0, 0.0, 0.0])
table.set_column("p0c", [0.0, 0.0, 0.0])
table.set_column("alpha_x", [0.0, 0.0, 0.0])
table.set_column("beta_x", [0.0, 0.0, 0.0])
table.set_column("eta_x", [0.0, 0.0, 0.0])
table.set_column("etap_x", [0.0, 0.0, 0.0])
table.set_column("psi_x", [0.0, 0.0, 0.0])
table.set_column("alpha_y", [0.0, 0.0, 0.0])
table.set_column("beta_y", [0.0, 0.0, 0.0])
table.set_column("eta_y", [0.0, 0.0, 0.0])
table.set_column("etap_y", [0.0, 0.0, 0.0])
table.set_column("psi_y", [0.0, 0.0, 0.0])
client.put("pva://my:table", table)
```
> **Note:**  
> The `MessagePackSerializable` object passed as the `value` parameter must contain a `key` attribute, which identifies the EPICS field (PV) to update, and a corresponding value (or structure) representing the data to be written to that field. This ensures that both the target PV and the data to update are clearly specified and correctly serialized

---

### Monitoring

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

> **Note:** Monitors are automatically removed from the K2EG gateway when there are no consumers for a specific PV anymore.

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

---

#### `stop_monitor(pv_name: str)`
Removes the monitor for a specific PV.

- **Parameters:**
  - `pv_name` (str): The name of the PV.

> **Note:** This method only stops the local K2EG Python library listener for PV events.

---

### Snapshot Operations

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
  - `properties` (**SnapshotProperties**): Snapshot configuration object that defines how the recurring snapshot will behave.

    **SnapshotProperties fields:**
    - **`snapshot_name`** (`str`):  
      Unique name for the snapshot operation.
    - **`pv_uri_list`** (`list[str]`):  
      List of PV URIs to include in the snapshot.
    - **`time_window`** (`int`, optional):  
      Time window in milliseconds for the snapshot acquisition.
    - **`repeat_delay`** (`int`, optional):  
      Delay in milliseconds between consecutive snapshots [not yet implemented].
    - **`sub_push_delay_msec`** (`int`, optional):  
      Specifies a sub-interval (in milliseconds) within the overall `time_window`. Instead of waiting until the end of the `time_window` to send all acquired events, the gateway will push partial results to the client every `sub_push_delay_msec`.  
      This reduces latency for large snapshots, especially when monitoring many PVs that update at a high rate, by delivering data incrementally rather than in a single batch at the end of the window.
    - **`triggered`** (`bool`, optional):  
      If `True`, the snapshot is triggered manually using `snapshost_trigger()`. If `False`, snapshots are taken automatically.
    - **Other fields**:  
      Additional fields may be supported depending on your K2EG deployment.

  - `handler` (Callable): Function to call asynchronously with the snapshot results. The handler receives two arguments: the snapshot ID (`str`) and the snapshot data (see [Snapshot Handler Return Type](#snapshot-handler-return-type)).
  - `timeout` (float, optional): Timeout in seconds for the operation.

> **Note:**  
> The `SnapshotProperties` object allows fine-grained control over how and when snapshots are taken, including which PVs to include, how often to repeat, and whether snapshots are triggered automatically or manually.

---

#### Snapshot Handler Parameters Type

Each time a snapshot is received, the handler is invoked with two parameters:

- `snapshot_name` (`str`): The name of the snapshot source.
- `data` (`Dict[str, Any]`): A dictionary containing the snapshot data with the following keys:

  - **`timestamp`** (`int`):  
    The Unix timestamp (in milliseconds) indicating when the snapshot was taken.
  - **`iteration`** (`int`):  
    The iteration number of the snapshot (useful for recurring snapshots).
  - **PV Name(s)** (`str`):  
    Each PV in the snapshot will have a key corresponding to its short name (e.g., `"variable:a"`, `"variable:b"`). The value for each PV key is the value of that process variable at the time of the snapshot.

**Example:**
```python
{
  "timestamp": 1716123456789,
  "iteration": 1,
  "variable:a": {epics field},
  "variable:b": {epics field},
}
```

**Usage in Handler:**
```python
def snapshot_handler(snapshot_id, snapshot_data):
    print(f"Snapshot {snapshot_id}: {snapshot_data}")
```

> **Note:**  
> The exact keys present in each dictionary depend on the PVs included in the snapshot request. The `timestamp` and `iteration` keys are always present.

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
def snapshot_handler(snapshot_id:str, snapshot_obj:Dict[str, Any]):
    print(f"Snapshot {snapshot_id}: {snapshot_obj}")

properties = SnapshotProperties(
    snapshot_name="my_snapshot",
    pv_uri_list=["pva://my:pv1", "ca://my:pv2"],
    triggered=False
    # ...other SnapshotProperties fields as needed...
)
client.snapshot_recurring(properties, snapshot_handler, timeout=5.0)

# Trigger a recurring snapshot manually
client.snapshost_trigger("my_snapshot", timeout=5.0)

# Stop a recurring snapshot
client.snapshot_stop("my_snapshot", timeout=5.0)
```