package com.datastax.oss.sga.ai.agents;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class GenAIToolKitAgent implements AgentCode  {

    @Override
    public List<Record> process(List<Record> record) {
        log.info("Processing {}", record);
        // TODO
        return new ArrayList<>(record);
    }

    @Override
    public void init(Map<String, Object> configuration) {
    }

    @Override
    public void start() {
    }

    @Override
    public void close() {
    }
}
