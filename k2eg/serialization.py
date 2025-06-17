import msgpack
import base64
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
    
    def to_base_64(self) -> str:
        """Pack `to_dict()` into msgpack bytes, then base64 encode."""
        return base64.b64encode(self.to_msgpack()).decode('utf-8')

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
    payload: Dict[str, any] = field(default_factory=dict)

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

    def set_column(self, label: str, data: List[Any]) -> "NTTable":
        """
        Update values for an existing column.
        - label: the column name (must already exist)
        - data: new list of values, one per row (length must match)
        """
        if label not in self.labels:
            raise ValueError(f"Column label '{label}' is not defined")
        self.payload[label] = data
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.key:  self.payload
        }
        
        
        