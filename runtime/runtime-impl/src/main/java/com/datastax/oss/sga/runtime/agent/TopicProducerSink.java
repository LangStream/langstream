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

import com.datastax.oss.sga.api.runner.code.AgentInfo;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;

import java.util.List;
import java.util.Map;

public class TopicProducerSink implements AgentSink {

    private final TopicProducer producer;
    private CommitCallback callback;

    @Override
    public String agentType() {
        return "topic-sink";
    }

    public TopicProducerSink(TopicProducer producer) {
        this.producer = producer;
    }

    @Override
    public void setCommitCallback(CommitCallback callback) {
        this.callback = callback;
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
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
    public void write(List<Record> records) throws Exception {
        producer.write(records);
        callback.commit(records);
    }

    @Override
    public String toString() {
        return "TopicProducerSink{" +
                "producer=" + producer +
                '}';
    }

    @Override
    public AgentInfo getInfo() {
        return new AgentInfo(agentType(), Map.of("producer", producer.getInfo()), producer.getTotalIn(), null);
    }
}
