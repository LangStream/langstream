#
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

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, List, Tuple, Dict, Union, Optional

__all__ = [
    "Record",
    "RecordType",
    "Agent",
    "Source",
    "Sink",
    "Processor",
]


class Record(ABC):
    """The Record interface"""

    @abstractmethod
    def key(self):
        """Get the record key."""
        pass

    @abstractmethod
    def value(self):
        """Get the record value."""
        pass

    @abstractmethod
    def origin(self) -> str:
        """Get the origin of the record."""
        pass

    @abstractmethod
    def timestamp(self) -> int:
        """Get the timestamp of the record."""
        pass

    @abstractmethod
    def headers(self) -> List[Tuple[str, Any]]:
        """Get the record headers."""
        pass


RecordType = Union[Record, dict, list, tuple]


class Agent(ABC):
    """The Agent interface"""

    def init(self, config: Dict[str, Any]):
        """Initialize the agent from the given configuration."""
        pass

    def start(self):
        """Start the agent."""
        pass

    def close(self):
        """Close the agent."""
        pass

    def agent_info(self) -> Dict[str, Any]:
        """Return the agent information."""
        return {}


class Source(Agent):
    """The Source agent interface

    A Source agent is used by the runtime to read Records.
    """

    @abstractmethod
    def read(self) -> List[RecordType]:
        """The Source agent generates records and returns them as list of records.

        :returns: the list of records.
        The records must either respect the Record API contract (have methods value(),
        key() and so on) or be a dict or tuples/list.
        If the records are dict, the keys if present shall be "value", "key",
        "headers", "origin" and "timestamp".
        Eg:
        * if you return [{"value": "foo"}] a record Record(value="foo") will be built.
        If the records are tuples/list, the framework will automatically construct
        Record objects from them with the values in the following order : value, key,
        headers, origin, timestamp.
        Eg:
        * if you return [("foo",)] a record Record(value="foo") will be built.
        * if you return [("foo", "bar")] a record Record(value="foo", key="bar") will
        be built.
        """
        pass

    def commit(self, record: Record):
        """Called by the framework to indicate that a record has been successfully
        processed."""
        pass

    def permanent_failure(self, record: Record, error: Exception):
        """Called by the framework to indicate that the agent has permanently failed to
        process a record.
        The Source agent may send the record to a dead letter queue or raise an error.
        """
        raise error


class Processor(Agent):
    """The Processor agent interface

    A Processor agent is used by the runtime to process Records.
    """

    @abstractmethod
    def process(
        self, record: Record
    ) -> Union[List[RecordType], Future[List[RecordType]]]:
        """The agent processes a record and returns a list of new records.

        :returns: the list of records or a concurrent.futures.Future that will complete
        with the list of records.
        When the processing is successful, the output records must either respect the
        Record API contract (have methods value(), key() and so on) or be a dict or
        tuples/list.
        If the records are dict, the keys if present shall be "value", "key",
        "headers", "origin" and "timestamp".
        Eg:
        * if you return {"value": "foo"} a record Record(value="foo") will be built.
        If the output records are tuples/list, the framework will automatically
        construct Record objects from them with the values in the following order :
        value, key, headers, origin, timestamp.
        Eg:
        * if you return ("foo",) a record Record(value="foo") will be built.
        * if you return ("foo", "bar") a record Record(value="foo", key="bar") will be
        built.
        """
        pass


class Sink(Agent):
    """The Sink agent interface

    A Sink agent is used by the runtime to write Records.
    """

    @abstractmethod
    def write(self, record: Record) -> Optional[Future[None]]:
        """The Sink agent receives records from the framework and typically writes them
        to an external service.
        For a synchronous result, return None/nothing if successful or otherwise raise
        an Exception.
        For an asynchronous result, return a concurrent.futures.Future.

        :returns: nothing if the write is successful or a concurrent.futures.Future
        """
        pass
