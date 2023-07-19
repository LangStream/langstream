package com.datastax.oss.sga.ai.kafkaconnect;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaConnectSinkAgent implements AgentSink {
    @Override
    public void write(List<Record> records) throws Exception {
        log.info("Received {} records", records.size());
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        log.info("Starting Kafka Connect Sink Agent with configuration: {}", configuration);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {
    }
}
