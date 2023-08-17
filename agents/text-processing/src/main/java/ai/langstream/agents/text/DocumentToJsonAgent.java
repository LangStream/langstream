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
package ai.langstream.agents.text;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DocumentToJsonAgent extends SingleRecordAgentProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String textField;
    private boolean copyProperties;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        this.textField = configuration.getOrDefault("text-field", "text").toString();
        this.copyProperties = Boolean.parseBoolean(configuration.getOrDefault("copy-properties", "true").toString());
    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        Object value = record.value();
        String stream = Utils.toText(value);

        Map<String, Object> asJson = new HashMap<>();
        asJson.put(textField, stream);
        if (copyProperties) {
            record.headers().forEach(h -> asJson.put(h.key(), h.value()));
        }

        return List.of(SimpleRecord
                .copyFrom(record)
                .value(MAPPER.writeValueAsString(asJson))
                .build());

    }
}
