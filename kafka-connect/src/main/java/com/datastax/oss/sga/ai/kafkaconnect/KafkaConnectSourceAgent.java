package com.datastax.oss.sga.ai.kafkaconnect;

import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaConnectSourceAgent implements AgentSource {

    @Override
    public List<Record> read() throws Exception {
        return List.of();
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        log.info("Committing");
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        log.info("Starting Kafka Connect Source Agent with configuration: {}", configuration);
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void close() throws Exception {
    }
}
