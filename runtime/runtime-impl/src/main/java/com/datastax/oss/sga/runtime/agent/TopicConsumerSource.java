package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class TopicConsumerSource implements AgentSource {

    private final TopicConsumer consumer;
    private final TopicProducer deadLetterQueueProducer;

    public TopicConsumerSource(TopicConsumer consumer,
                               TopicProducer deadLetterQueueProducer) {
        this.consumer = consumer;
        this.deadLetterQueueProducer = deadLetterQueueProducer;
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
    public void permanentFailure(Record record, Exception error) throws Exception {
        // DLQ
        log.error("Sending record to DLQ: {}", record);
        deadLetterQueueProducer.write(List.of(record));
    }

    @Override
    public void start() throws Exception {
        consumer.start();
        deadLetterQueueProducer.start();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
        deadLetterQueueProducer.close();
    }

    @Override
    public String toString() {
        return "TopicConsumerSource{" +
                "consumer=" + consumer +
                '}';
    }
}
