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
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicProducer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TopicProducerSink extends AbstractAgentCode implements AgentSink {

    private final TopicProducer producer;

    public TopicProducerSink(TopicProducer producer) {
        this.producer = producer;
    }

    @Override
    public void init(Map<String, Object> configuration) {
        // the producer is already initialized
    }

    @Override
    public void start() throws Exception {
        producer.start();
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

    @Override
    public CompletableFuture<?> write(Record records) {
        processed(1, 0);
        return producer.write(records);
    }

    @Override
    public String toString() {
        return "TopicProducerSink{" + "producer=" + producer + '}';
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("producer", producer.getInfo());
    }
}
