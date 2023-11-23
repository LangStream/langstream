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
package ai.langstream.agents.flow;

import ai.langstream.ai.agents.commons.JsonRecord;
import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.ai.agents.commons.jstl.predicate.JstlPredicate;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.util.ConfigurationUtils;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogEventProcessor extends AbstractAgentCode implements AgentProcessor {

    record FieldDefinition(String name, JstlEvaluator<Object> expressionEvaluator) {}

    private final List<FieldDefinition> fields = new ArrayList<>();
    private Template messageTemplate;
    private JstlPredicate predicate;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        String when = ConfigurationUtils.getString("when", "true", configuration);
        String message = ConfigurationUtils.getString("message", "", configuration);
        predicate = new JstlPredicate(when);

        if (!message.isEmpty()) {
            messageTemplate = Mustache.compiler().compile(message);
        } else {
            messageTemplate = null;
        }

        List<Map<String, Object>> fields =
                (List<Map<String, Object>>) configuration.getOrDefault("fields", List.of());
        fields.forEach(
                r -> {
                    String name = ConfigurationUtils.getString("name", "", r);
                    String expression = ConfigurationUtils.getString("expression", "", r);
                    log.info("Emitting field with name {} computed as {}", name, expression);
                    JstlEvaluator<Object> expressionEvaluator =
                            new JstlEvaluator<>("${" + expression + "}", Object.class);
                    this.fields.add(new FieldDefinition(name, expressionEvaluator));
                });
    }

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    private void logRecord(Record originalRecord) {
        MutableRecord mutableRecord = MutableRecord.recordToMutableRecord(originalRecord, true);
        if (!predicate.test(mutableRecord)) {
            return;
        }
        if (fields.isEmpty() && messageTemplate == null) {
            log.info("{}", originalRecord);
            return;
        }

        if (!fields.isEmpty()) {
            // using LinkedHashMap in order to keep the order
            Map<String, Object> values = new LinkedHashMap<>();
            for (FieldDefinition field : fields) {
                values.put(field.name, field.expressionEvaluator.evaluate(mutableRecord));
            }
            log.info("{}", values);
        }

        if (messageTemplate != null) {
            JsonRecord jsonRecord = mutableRecord.toJsonRecord();
            log.info("{}", messageTemplate.execute(jsonRecord));
        }
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record r : records) {
            try {
                logRecord(r);
            } catch (RuntimeException error) {
                log.error("Error while processing record {}", r, error);
            } finally {
                recordSink.emitSingleResult(r, r);
            }
        }
    }
}
