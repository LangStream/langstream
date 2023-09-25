package com.datastax.oss.streaming.ai.streaming;

import com.datastax.oss.streaming.ai.TransformContext;

public interface StreamingAnswersConsumer {
    void streamAnswerChunk(
            int index, String message, boolean last, TransformContext outputMessage);

    default void close() {
    }
}
