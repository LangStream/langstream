/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.api.runner.code;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class SingleRecordAgentProcessor extends AbstractAgentCode
        implements AgentProcessor {

    private final AtomicLong errors = new AtomicLong();

    public abstract List<Record> processRecord(Record record) throws Exception;

    @Override
    public final void process(List<Record> records, RecordSink recordSink) {
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("Records cannot be null or empty");
        }
        for (Record record : records) {
            try {
                List<Record> process = processRecord(record);
                processed(1, process.size());
                recordSink.emit(new SourceRecordAndResult(record, process, null));
            } catch (Throwable error) {
                log.error("Error processing record: {}", record, error);
                errors.incrementAndGet();
                recordSink.emit(new SourceRecordAndResult(record, null, error));
            }
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("errors", errors.get());
    }
}
