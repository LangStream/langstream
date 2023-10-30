#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from dataclasses import dataclass
from typing import Any, List, Tuple

from .api import Record

__all__ = ["SimpleRecord", "AvroValue"]


class SimpleRecord(Record):
    """A basic implementation of the Record interface"""

    def __init__(
        self,
        value,
        key=None,
        headers: List[Tuple[str, Any]] = None,
        origin: str = None,
        timestamp: int = None,
    ):
        self._value = value
        self._key = key
        self._headers = headers or []
        self._origin = origin
        self._timestamp = timestamp

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self) -> List[Tuple[str, Any]]:
        return self._headers

    def origin(self) -> str:
        return self._origin

    def timestamp(self) -> int:
        return self._timestamp

    def __str__(self):
        return (
            f"SimpleRecord(value={self._value}, key={self._key}, "
            f"origin={self._origin},timestamp={self._timestamp}, "
            f"headers={self._headers})"
        )

    def __repr__(self):
        return self.__str__()


@dataclass
class AvroValue(object):
    schema: dict
    value: Any
