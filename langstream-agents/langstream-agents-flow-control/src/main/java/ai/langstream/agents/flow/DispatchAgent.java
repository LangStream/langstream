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
public class DispatchAgent extends AbstractAgentCode implements AgentProcessor {

    record Route(String destination, boolean drop, JstlPredicate predicate) {}

    private final List<Route> routes = new ArrayList<>();
    private final Map<String, TopicProducer> producers = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        List<Map<String, Object>> routes =
                (List<Map<String, Object>>) configuration.getOrDefault("routes", List.of());
        routes.forEach(
                r -> {
                    String when = ConfigurationUtils.getString("when", "", r);
                    String destination = ConfigurationUtils.getString("destination", "", r);
                    String action = ConfigurationUtils.getString("action", "dispatch", r);
                    final boolean drop;
                    switch (action) {
                        case "dispatch":
                            drop = false;
                            break;
                        case "drop":
                            drop = true;
                            break;
                        default:
                            throw new IllegalArgumentException();
                    }
                    if (drop) {
                        log.info("Condition: \"{}\", action = drop", when);
                        if (!destination.isEmpty()) {
                            throw new IllegalStateException(
                                    "drop action cannot have a destination");
                        }
                    } else {
                        log.info(
                                "Condition: \"{}\", action = dispatch, destination: {}",
                                when,
                                destination);
                    }
                    this.routes.add(new Route(destination, drop, new JstlPredicate(when)));
                });
    }

    @Override
    public void start() throws Exception {
        routes.forEach(
                r -> {
                    String topic = r.destination;
                    if (topic != null && !topic.isEmpty()) {
                        TopicProducer producer =
                                agentContext
                                        .getTopicConnectionProvider()
                                        .createProducer(
                                                agentContext.getGlobalAgentId(), topic, Map.of());
                        producer.start();
                        producers.put(topic, producer);
                    }
                });
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record record : records) {
            processRecord(record, recordSink);
        }
    }

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    public void processRecord(Record record, RecordSink recordSink) {
        try {
            MutableRecord context = MutableRecord.recordToMutableRecord(record, true);

            for (Route r : routes) {
                boolean test = r.predicate.test(context);
                if (test) {
                    if (r.drop) {
                        if (log.isDebugEnabled()) {
                            log.debug("Discarding record {} - action=drop", record);
                        }
                        recordSink.emit(new SourceRecordAndResult(record, List.of(), null));
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Dispatching record {} to topic {}", record, r.destination);
                        }
                        TopicProducer topicProducer = producers.get(r.destination);
                        topicProducer
                                .write(record)
                                .whenComplete(
                                        (__, e) -> {
                                            if (e != null) {
                                                log.error(
                                                        "Error writing record to topic {}",
                                                        r.destination,
                                                        e);
                                                recordSink.emit(
                                                        new SourceRecordAndResult(record, null, e));
                                            } else {
                                                // the record is to be marked as processed, but not
                                                // emitted to the
                                                // next agent
                                                recordSink.emit(
                                                        new SourceRecordAndResult(
                                                                record, List.of(), null));
                                            }
                                        });
                    }
                    return;
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Sending record {} to the default destination", record);
            }
            recordSink.emit(new SourceRecordAndResult(record, List.of(record), null));
        } catch (Throwable error) {
            log.error("Error processing record: {}", record, error);
            recordSink.emit(new SourceRecordAndResult(record, null, error));
        }
    }

    @Override
    public void close() throws Exception {
        producers.forEach(
                (destination, producer) -> {
                    log.info("Closing producer for destination {}", destination);
                    try {
                        producer.close();
                    } catch (Exception e) {
                        log.error("Error closing producer for destination {}", destination, e);
                    }
                });
    }
}
