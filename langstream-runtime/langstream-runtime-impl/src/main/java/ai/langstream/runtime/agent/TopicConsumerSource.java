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
package ai.langstream.runtime.agent;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicConsumerSource extends AbstractAgentCode implements AgentSource {

    private final TopicConsumer consumer;
    private final TopicProducer deadLetterQueueProducer;

    public TopicConsumerSource(TopicConsumer consumer, TopicProducer deadLetterQueueProducer) {
        this.consumer = consumer;
        this.deadLetterQueueProducer = deadLetterQueueProducer;
    }

    @Override
    public List<Record> read() throws Exception {
        List<Record> result = consumer.read();
        processed(0, result.size());
        return result;
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        consumer.commit(records);
    }

    @Override
    public void permanentFailure(Record record, Exception error) {
        // DLQ
        log.error("Permanent failure on record {}", record, error);
        deadLetterQueueProducer.write(record).join();
    }

    @Override
    public void start() throws Exception {
        consumer.start();
        log.info("Starting consumer {}", consumer);
        deadLetterQueueProducer.start();
    }

    @Override
    public void close() throws Exception {
        log.info("Closing consumer {}", consumer);
        consumer.close();
        deadLetterQueueProducer.close();
    }

    @Override
    public String toString() {
        return "TopicConsumerSource{" + "consumer=" + consumer + '}';
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("consumer", consumer.getInfo());
    }
}
