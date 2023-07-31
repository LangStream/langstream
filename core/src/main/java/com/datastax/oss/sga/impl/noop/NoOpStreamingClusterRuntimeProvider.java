package com.datastax.oss.sga.impl.noop;

import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntimeProvider;
import com.datastax.oss.sga.api.runtime.Topic;

public class NoOpStreamingClusterRuntimeProvider implements StreamingClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "noop".equals(type);
    }

    public record SimpleTopic(String name, boolean implicit) implements Topic {
        @Override
        public String topicName() {
            return name;
        }

        @Override
        public boolean implicit() {
            return this.implicit;
        }
    }
    @Override
    public StreamingClusterRuntime getImplementation() {
        return new StreamingClusterRuntime() {
            @Override
            public Topic createTopicImplementation(TopicDefinition topicDefinition, ExecutionPlan applicationInstance) {
                return new SimpleTopic(topicDefinition.getName(), topicDefinition.isImplicit());
            }
        };
    }
}
