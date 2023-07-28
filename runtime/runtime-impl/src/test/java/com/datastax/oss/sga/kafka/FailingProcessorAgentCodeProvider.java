package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class FailingProcessorAgentCodeProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "mock-failing-processor".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new SingleRecordAgentProcessor() {

            String failOnContent;
            @Override
            public void init(Map<String, Object> configuration) throws Exception {
                failOnContent = configuration.getOrDefault("fail-on-content", "").toString();
            }

            @Override
            public List<Record> processRecord(Record record) throws Exception {
                log.info("Processing record value {}, failOnContent {}", record.value(), failOnContent);
                if (Objects.equals(record.value(), failOnContent)) {
                    throw new RuntimeException("Failing on content: " + failOnContent);
                }
                return List.of(record);
            }
        };
    }
}
