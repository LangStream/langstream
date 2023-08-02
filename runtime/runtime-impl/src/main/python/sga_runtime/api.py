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
from typing import Any, List, Tuple, Dict, NamedTuple


class Record(ABC):
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


class Agent(ABC):
    def init(self, config: Dict[str, Any]):
        pass

    def start(self):
        pass

    def close(self):
        pass


class Source(Agent):
    @abstractmethod
    def read(self) -> List[Record]:
        pass

    def commit(self, records: List[Record]):
        pass


class Processor(Agent):
    @abstractmethod
    def process(self, records: List[Record]) -> List[Tuple[Record, List[Record]]]:
        pass


class CommitCallback(ABC):
    @abstractmethod
    def commit(self, records: List[Record]):
        pass


class Sink(Agent):
    @abstractmethod
    def write(self, records: List[Record]):
        pass

    @abstractmethod
    def set_commit_callback(self, commit_callback: CommitCallback):
        pass
