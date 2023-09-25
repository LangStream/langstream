package com.datastax.oss.streaming.ai.streaming;

public interface StreamingAnswersConsumerFactory {
    StreamingAnswersConsumer create(String topicName);
}
