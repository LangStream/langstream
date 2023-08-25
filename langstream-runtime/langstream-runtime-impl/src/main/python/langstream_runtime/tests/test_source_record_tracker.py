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

from typing import List

from langstream import Source, Record, SimpleRecord
from langstream_runtime.source_record_tracker import SourceRecordTracker


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
