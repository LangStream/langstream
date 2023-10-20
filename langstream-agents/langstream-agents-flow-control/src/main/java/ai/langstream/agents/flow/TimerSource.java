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
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimerSource extends AbstractAgentCode implements AgentSource {

    record FieldDefinition(String name, JstlEvaluator<Object> expressionEvaluator) {}

    private static final int MAX_QUEUE_SIZE = 2;
    private final List<FieldDefinition> fields = new ArrayList<>();
    private final BlockingQueue<Record> queue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    private int periodSeconds;
    private ScheduledExecutorService executorService;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        periodSeconds = ConfigurationUtils.getInt("period-seconds", 60, configuration);
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
        executorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "timer-source-" + agentId());
                            }
                        });
    }

    private void emitRecord() {
        try {
            MutableRecord mutableRecord =
                    MutableRecord.recordToMutableRecord(SimpleRecord.of("{}", "{}"), true);
            Map<String, Object> values = new HashMap<>();
            for (FieldDefinition field : fields) {
                values.put(field.name, field.expressionEvaluator.evaluate(mutableRecord));
            }
            values.forEach(
                    (fieldName, fieldValue) -> {
                        mutableRecord.setResultField(fieldValue, fieldName, null, null, null);
                    });
            Record record = MutableRecord.mutableRecordToRecord(mutableRecord).orElseThrow();
            queue.put(record);
            if (log.isDebugEnabled()) {
                log.debug("Generated record {}", record);
            }
        } catch (Throwable e) {
            log.error("Error while emitting record", e);
        }
    }

    @Override
    public void start() throws Exception {
        executorService.scheduleAtFixedRate(
                this::emitRecord, periodSeconds, periodSeconds, TimeUnit.SECONDS);
    }

    @Override
    public List<Record> read() throws Exception {
        try {
            // this is a blocking operation, the main loop will be blocked until a record is
            // available, but we want to let the agent shutdown gracefully in case there it nothing
            // to serve
            Record taken = queue.poll(500, TimeUnit.MILLISECONDS);
            if (taken == null) {
                return List.of();
            }
            return List.of(taken);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        // ignore
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
    }
}
