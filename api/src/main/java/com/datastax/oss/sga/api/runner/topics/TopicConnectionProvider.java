package com.datastax.oss.sga.api.runner.topics;

import java.util.Map;

public interface TopicConnectionProvider {
    TopicConsumer createConsumer(String agentId, Map<String, Object> config);

    TopicProducer createProducer(String agentId, Map<String, Object> config);
}
