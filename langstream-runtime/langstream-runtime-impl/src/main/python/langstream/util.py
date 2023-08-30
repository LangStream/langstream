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

from abc import abstractmethod
from typing import Any, List, Tuple, Union

from .api import Record, Processor

__all__ = ["SimpleRecord", "SingleRecordProcessor"]


class SimpleRecord(Record):
    """A basic implementation of the Record interface"""

    def __init__(
        self,
        value,
        key=None,
        origin: str = None,
        timestamp: int = None,
        headers: List[Tuple[str, Any]] = None,
    ):
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
        return (
            f"Record(value={self._value}, key={self._key}, origin={self._origin}, "
            f"timestamp={self._timestamp}, headers={self._headers})"
        )

    def __repr__(self):
        return self.__str__()


class SingleRecordProcessor(Processor):
    """A Processor that processes records one-by-one"""

    @abstractmethod
    def process_record(self, record: Record) -> List[Record]:
        """Process one record and return a list of records or raise an exception"""
        pass

    def process(
        self, records: List[Record]
    ) -> List[Tuple[Record, Union[List[Record], Exception]]]:
        results = []
        for record in records:
            try:
                processed = self.process_record(record)
                results.append((record, processed))
            except Exception as e:
                results.append((record, e))
        return results
