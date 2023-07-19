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
    public void commit() throws Exception {
        consumer.commit();
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
