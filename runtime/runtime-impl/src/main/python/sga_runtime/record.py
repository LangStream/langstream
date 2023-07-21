from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class AbstractRecord(ABC):
    @abstractmethod
    def key(self):
        pass

    @abstractmethod
    def value(self):
        pass

    @abstractmethod
    def origin(self) -> str:
        pass

    @abstractmethod
    def timestamp(self) -> int:
        pass

    @abstractmethod
    def headers(self) -> List[Tuple[str, Any]]:
        pass


class Record(AbstractRecord):
    def __init__(self, value, key=None, origin: str = None, timestamp: int = None,
                 headers: List[Tuple[str, Any]] = None):
        self._value = value
        self._key = key
        self._origin = origin
        self._timestamp = timestamp
        self._headers = headers or []

    def key(self):
        return self._key

    def value(self):
        return self._value

    def origin(self) -> str:
        return self._origin

    def timestamp(self) -> int:
        return self._timestamp

    def headers(self) -> List[Tuple[str, Any]]:
        return self._headers

    def __str__(self):
        return f"Record(value={self._value}, key={self._key}, origin={self._origin}, timestamp={self._timestamp}, " \
               f"headers={self._headers})"
