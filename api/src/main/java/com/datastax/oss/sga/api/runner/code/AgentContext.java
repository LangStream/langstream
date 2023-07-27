package com.datastax.oss.sga.api.runner.code;

import com.datastax.oss.sga.api.runner.topics.TopicAdmin;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicConsumerProvider;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;

public interface AgentContext {
    TopicConsumer getTopicConsumer();

    TopicProducer getTopicProducer();

    String getAgentId();

    TopicAdmin getTopicAdmin();

    TopicConsumerProvider getTopicConsumerProvider();
}
