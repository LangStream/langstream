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
from dataclasses import dataclass
from typing import Any, List, Tuple, Dict, Union, Optional

__all__ = [
    "Record",
    "RecordType",
    "AgentContext",
    "Agent",
    "Source",
    "Sink",
    "Processor",
    "CommitCallback",
    "TopicConsumer",
    "TopicProducer",
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


RecordType = Union[Record, list, tuple]


class TopicConsumer(ABC):
    """The topic consumer interface"""

    def start(self):
        """Start the consumer."""
        pass

    def close(self):
        """Close the consumer"""
        pass

    def read(self) -> List[Record]:
        """Read records from the topic."""
        return []

    def commit(self, records: List[Record]):
        """Commit records."""
        pass

    def get_native_consumer(self) -> Any:
        """Return the native wrapped consumer"""
        pass

    def get_info(self) -> Dict[str, Any]:
        """Return the consumer info"""
        return {}


class TopicProducer(ABC):
    """The topic producer interface"""

    def start(self):
        """Start the producer."""
        pass

    def close(self):
        """Close the producer."""
        pass

    def write(self, records: List[Record]):
        """Write records to the topic."""
        pass

    def get_native_producer(self) -> Any:
        """Return the native wrapped producer"""
        pass

    def get_info(self) -> Dict[str, Any]:
        """Return the producer info"""
        return {}


@dataclass
class AgentContext(object):
    """The Agent context"""

    topic_consumer: Optional[TopicConsumer] = None
    topic_producer: Optional[TopicProducer] = None
    global_agent_id: Optional[str] = None


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

    def set_context(self, context: AgentContext):
        """Set the agent context."""
        pass


class Source(Agent):
    """The Source agent interface

    A Source agent is used by the runtime to read Records.
    """

    @abstractmethod
    def read(self) -> List[RecordType]:
        """The Source agent generates records and returns them as list of records.

        :returns: the list of records. The records must either respect the Record
        API contract (have methods value(), key() and so on) or be tuples/list.
        If the records are tuples/list, the framework will automatically construct
        Record objects from them with the values in the following order : value, key,
        headers, origin, timestamp.
        Eg:
        * if you return [("foo",)] a record Record(value="foo") will be built.
        * if you return [("foo", "bar")] a record Record(value="foo", key="bar") will
        be built.
        """
        pass

    def commit(self, records: List[Record]):
        """Called by the framework to indicate the records that have been successfully
        processed."""
        pass

    def permanent_failure(self, record: Record, error: Exception):
        """Called by the framework to indicate that the agent has permanently failed to
        process the record.
        The Source agent may send the records to a dead letter queue or raise an error.
        """
        raise error


class Processor(Agent):
    """The Processor agent interface

    A Processor agent is used by the runtime to process Records.
    """

    @abstractmethod
    def process(
        self, records: List[Record]
    ) -> List[Tuple[Record, Union[List[RecordType], Exception]]]:
        """The agent processes records and returns a list containing the associations of
        these records with the result of these record processing.
        The result of each record processing is a list of new records or an exception.
        The transactionality of the function is guaranteed by the runtime.

        :returns: the list of associations between an input record and the output
        records processed from it.
        Eg: [(input_record, [output_record1, output_record2])]
        If an input record cannot be processed, the associated element shall be an
        exception.
        Eg: [(input_record, RuntimeError("Could not process"))]
        When the processing is successful, the output records must either respect the
        Record API contract (have methods value(), key() and so on) or be tuples/list.
        If the output records are tuples/list, the framework will automatically
        construct Record objects from them with the values in the following order :
        value, key, headers, origin, timestamp.
        Eg:
        * if you return [(input_record, [("foo",)])] a record Record(value="foo") will
        be built.
        * if you return [(input_record, [("foo", "bar")])] a record
        Record(value="foo", key="bar") will be built.
        """
        pass


class CommitCallback(ABC):
    @abstractmethod
    def commit(self, records: List[Record]):
        """Called by a Sink to indicate the records that have been successfully
        written."""
        pass


class Sink(Agent):
    """The Sink agent interface

    A Sink agent is used by the runtime to write Records.
    """

    @abstractmethod
    def write(self, records: List[Record]):
        """The Sink agent receives records from the framework and typically writes them
        to an external service."""
        pass

    @abstractmethod
    def set_commit_callback(self, commit_callback: CommitCallback):
        """Called by the framework to specify a CommitCallback that shall be used by the
        Sink to indicate the records that have been written."""
        pass
