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

from typing import List, Tuple

from ..api import CommitCallback, Record, Source


class SourceRecordTracker(CommitCallback):
    def __init__(self, source: Source):
        self.source = source
        self.sink_to_source_mapping = {}
        self.remaining_sink_records_for_source_record = {}

    def commit(self, sink_records: List[Record]):
        source_records_to_commit = []
        for record in sink_records:
            if record in self.sink_to_source_mapping:
                source_record = self.sink_to_source_mapping[record]
                self.remaining_sink_records_for_source_record[source_record] -= 1
                if self.remaining_sink_records_for_source_record[source_record] == 0:
                    source_records_to_commit.append(source_record)

        if hasattr(self.source, 'commit'):
            self.source.commit(source_records_to_commit)
        # forget about this batch records
        for record in sink_records:
            del self.sink_to_source_mapping[record]

    def track(self, sink_records: List[Tuple[Record, List[Record]]]):
        # map each sink record to the original source record
        for source_record_and_result in sink_records:
            source_record = source_record_and_result[0]
            result_records = source_record_and_result[1]
            if not isinstance(result_records, Exception):
                self.remaining_sink_records_for_source_record[source_record] = len(result_records)
                for sink_record in result_records:
                    self.sink_to_source_mapping[sink_record] = source_record
