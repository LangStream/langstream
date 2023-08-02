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
package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;

import java.util.List;
import java.util.Map;

public class TopicConsumerSource implements AgentSource {

    private final TopicConsumer consumer;

    public TopicConsumerSource(TopicConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        // the consumer is already initialized
    }

    @Override
    public List<Record> read() throws Exception {
        return consumer.read();
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        consumer.commit(records);
    }

    @Override
    public void start() throws Exception {
        consumer.start();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }

    @Override
    public String toString() {
        return "TopicConsumerSource{" +
                "consumer=" + consumer +
                '}';
    }
}
