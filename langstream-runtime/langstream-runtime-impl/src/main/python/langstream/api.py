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
from typing import Any, List, Tuple, Dict, Union

__all__ = [
    'Record',
    'Agent',
    'Source',
    'Sink',
    'Processor',
    'CommitCallback'
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


class Source(Agent):
    """The Source agent interface

    A Source agent is used by the runtime to read Records.
    """

    @abstractmethod
    def read(self) -> List[Record]:
        """The Source agent generates records and returns them as list of records."""
        pass

    def commit(self, records: List[Record]):
        """Called by the framework to indicate the records that have been successfully processed."""
        pass

    def permanent_failure(self, record: Record, error: Exception):
        """Called by the framework to indicate that the agent has permanently failed to process the record.
        The Source agent may send the records to a dead letter queue or raise an error.
        """
        raise error


class Processor(Agent):
    """The Processor agent interface

    A Processor agent is used by the runtime to process Records.
    """
    @abstractmethod
    def process(self, records: List[Record]) -> List[Tuple[Record, Union[List[Record], Exception]]]:
        """The agent processes records and returns a list containing the association of these records and the result
        of these record processing.
        The result of each record processing is a list of new records or an exception.
        The transactionality of the function is guaranteed by the runtime.
        """
        pass


class CommitCallback(ABC):
    @abstractmethod
    def commit(self, records: List[Record]):
        """Called by a Sink to indicate the records that have been successfully written."""
        pass


class Sink(Agent):
    """The Sink agent interface

    A Sink agent is used by the runtime to write Records.
    """

    @abstractmethod
    def write(self, records: List[Record]):
        """The Sink agent receives records from the framework and typically writes them to an external service."""
        pass

    @abstractmethod
    def set_commit_callback(self, commit_callback: CommitCallback):
        """Called by the framework to specify a CommitCallback that shall be used by the Sink to indicate the
        records that have been written."""
        pass
