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
