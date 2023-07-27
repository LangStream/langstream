from typing import List

from sga_runtime.api import Source, Record
from sga_runtime.simplerecord import SimpleRecord
from sga_runtime.source_record_tracker import SourceRecordTracker


def test_tracker():
    source = MySource()
    tracker = SourceRecordTracker(source)

    source_record = SimpleRecord("sourceValue")
    sink_record = SimpleRecord("sinkValue")

    tracker.track([(source_record, [sink_record])])
    tracker.commit([sink_record])

    assert source.committed == [source_record]


def test_chunking():
    source = MySource()
    tracker = SourceRecordTracker(source)

    source_record = SimpleRecord("sourceValue")
    sink_record = SimpleRecord("sinkValue")
    sink_record2 = SimpleRecord("sinkValue2")

    tracker.track([(source_record, [sink_record, sink_record2])])

    # the sink commits only 1 of the 2 records
    tracker.commit([sink_record])
    assert source.committed == []

    tracker.commit([sink_record2])
    assert source.committed == [source_record]


class MySource(Source):
    def __init__(self):
        self.committed = []

    def read(self) -> List[Record]:
        return []

    def commit(self, records: List[Record]):
        self.committed.extend(records)
