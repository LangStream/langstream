/**
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
package com.datastax.oss.sga.api.runner.code;

import java.util.ArrayList;
import java.util.List;

public abstract class SingleRecordAgentProcessor implements AgentProcessor {

    public abstract List<Record> processRecord(Record record) throws Exception;

    @Override
    public final List<SourceRecordAndResult> process(List<Record> records) {
        List<SourceRecordAndResult> result = new ArrayList<>();
        for (Record record : records) {
            try {
                List<Record> process = processRecord(record);
                if (!process.isEmpty()) {
                    result.add(new SourceRecordAndResult(record, process, null));
                }
            } catch (Throwable error) {
                result.add(new SourceRecordAndResult(record, null, error));
            }
        }
        return result;
    }
}
