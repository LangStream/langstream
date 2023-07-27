package com.datastax.oss.sga.api.runner.topics;

import java.util.Map;

public interface TopicConsumerProvider {
    TopicConsumer createConsumer(String agentId, Map<String, Object> config);
}
