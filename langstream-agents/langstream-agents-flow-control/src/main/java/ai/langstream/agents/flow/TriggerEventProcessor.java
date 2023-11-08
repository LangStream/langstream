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

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.ai.agents.commons.jstl.predicate.JstlPredicate;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TriggerEventProcessor extends AbstractAgentCode implements AgentProcessor {

    record FieldDefinition(String name, JstlEvaluator<Object> expressionEvaluator) {}

    private final List<FieldDefinition> fields = new ArrayList<>();
    private TopicProducer topicProducer;
    private String destination;
    private JstlPredicate predicate;

    private boolean continueProcessing;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        destination = ConfigurationUtils.getString("destination", "", configuration);
        String when = ConfigurationUtils.getString("when", "true", configuration);
        continueProcessing =
                ConfigurationUtils.getBoolean("continue-processing", true, configuration);
        predicate = new JstlPredicate(when);
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

    private Record emitRecord(Record originalRecord) {
        MutableRecord mutableRecord =
                MutableRecord.recordToMutableRecord(originalRecord, true).copy();
        if (!predicate.test(mutableRecord)) {
            return null;
        }
        Map<String, Object> values = new HashMap<>();
        for (FieldDefinition field : fields) {
            values.put(field.name, field.expressionEvaluator.evaluate(mutableRecord));
        }
        values.forEach(
                (fieldName, fieldValue) -> {
                    mutableRecord.setResultField(fieldValue, fieldName, null, null, null);
                });
        Record record = MutableRecord.mutableRecordToRecord(mutableRecord).orElseThrow();
        log.info("Generated record {}", record);
        return record;
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record r : records) {
            processRecordAsync(r, recordSink);
        }
    }

    private void processRecordAsync(Record r, RecordSink recordSink) {
        Record newRecord;
        try {
            newRecord = emitRecord(r);
            if (newRecord == null) {
                log.info("Record {} does not match the predicate, skipping", r);
                // nothing to do
                recordSink.emitSingleResult(r, r);
                return;
            }
        } catch (RuntimeException error) {
            recordSink.emitError(r, error);
            return;
        }
        topicProducer
                .write(newRecord)
                .whenComplete(
                        (v, e) -> {
                            if (e != null) {
                                log.error("Error while writing record to topic", e);
                                recordSink.emitError(r, e);
                            } else {
                                if (continueProcessing) {
                                    // pass the source record as the result
                                    // the new record has been written to the side topic
                                    recordSink.emitSingleResult(r, r);
                                } else {
                                    // stop processing the message
                                    recordSink.emitEmptyList(r);
                                }
                            }
                        });
    }

    @Override
    public void start() throws Exception {
        topicProducer =
                agentContext
                        .getTopicConnectionProvider()
                        .createProducer(agentContext.getGlobalAgentId(), destination, Map.of());
        topicProducer.start();
    }

    @Override
    public void close() throws Exception {
        if (topicProducer != null) {
            topicProducer.close();
            topicProducer = null;
        }
    }
}
