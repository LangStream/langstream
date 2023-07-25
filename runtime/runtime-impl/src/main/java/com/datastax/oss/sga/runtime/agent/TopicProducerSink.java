package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;

import java.util.List;
import java.util.Map;

public class TopicProducerSink implements AgentSink {

    private final TopicProducer producer;
    private CommitCallback callback;

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
}
