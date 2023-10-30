package ai.langstream.impl.noop;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReader;

import java.util.Map;

public class NoopTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
    @Override
    public boolean supports(String streamingClusterType) {
        return "noop".equals(streamingClusterType);
    }

    @Override
    public TopicConnectionsRuntime getImplementation() {
        return NoopTopicConnectionsRuntime.INSTANCE;
    }

    private static class NoopTopicConnectionsRuntime implements TopicConnectionsRuntime {

        static final TopicConnectionsRuntime INSTANCE = new NoopTopicConnectionsRuntime();

        @Override
        public TopicConsumer createConsumer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
            throw new UnsupportedOperationException("You have not configured a streamingCluster for this application.");
        }

        @Override
        public TopicReader createReader(StreamingCluster streamingCluster, Map<String, Object> configuration, TopicOffsetPosition initialPosition) {
            throw new UnsupportedOperationException("You have not configured a streamingCluster for this application.");
        }

        @Override
        public TopicProducer createProducer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
            throw new UnsupportedOperationException("You have not configured a streamingCluster for this application.");
        }
    }
}
