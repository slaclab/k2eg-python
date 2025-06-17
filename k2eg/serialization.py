import msgpack
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Union

class MessagePackSerializable(ABC):
    """Base class: define msgpack (de)serialization contract."""
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Return a pure-Python dict representation."""
        ...

    def to_msgpack(self) -> bytes:
        """Pack `to_dict()` into msgpack bytes."""
        return msgpack.packb(self.to_dict(), use_bin_type=True)

@dataclass
class Scalar(MessagePackSerializable):
    """Wrap a single scalar value, always serialized as a map with a key."""
    key: str = field(default="value")
    payload: Any = field(default="")

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.key:  self.payload
        }


@dataclass
class Vector(MessagePackSerializable):
    """Wrap a 1D sequence of values, always serialized as a map with a key."""
    key: str = field(default="value")
    payload: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.key: self.payload
        }


@dataclass
class Generic(MessagePackSerializable):
    key: str = field(default="value")
    payload: Dict[str, Any] = field(default_factory=dict)
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: self.payload}

@dataclass
class NTTable(MessagePackSerializable):
    """
    EPICS NTTable format:
      - key: identifier
      - labels: list of column names
      - values: list of rows; each row is a list of values
    """
    key: str = field(default="value")
    labels: List[str] = field(default_factory=list)
    payload: List[List[Any]] = field(default_factory=list)

    def wrap(
        self,
        records: List[Union[Dict[str, Any], List[Any], Tuple[Any, ...]]]
    ) -> "NTTable":
        """
        Populate self.values from a list of dicts or sequences.
        - If record is dict: extract values in order of self.labels.
        - If sequence: must match len(self.labels).
        """
        self.values.clear()
        for rec in records:
            if isinstance(rec, dict):
                row = [rec[label] for label in self.labels]
            elif isinstance(rec, (list, tuple)):
                if len(rec) != len(self.labels):
                    raise ValueError("Row length does not match labels")
                row = list(rec)
            else:
                raise TypeError("Record must be dict or sequence")
            self.values.append(row)
        return self

    def add_column(self, label: str, data: List[Any]) -> "NTTable":
        """
        Add a new column to the table.
        
        - label: the column name
        - data: list of values, one per row
        - fmt: optional format specifier for this column
        """
        # If table is empty, initialize values with one-element rows
        if not self.payload:
            for val in data:
                self.payload.append([val])
        else:
            if len(data) != len(self.payload):
                raise ValueError(
                    f"Column length {len(data)} does not match row count {len(self.payload)}"
                )
            for row, val in zip(self.payload, data):
                row.append(val)

        # Register the new column
        self.labels.append(label)
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.key:  self.payload
        }
        
        
        