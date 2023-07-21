package com.datastax.oss.sga.ai.kafkaconnect;

public record ConsumerCommand(Command command, Object arg) {
    enum Command {
        COMMIT,
        PAUSE,
        RESUME,
        SEEK,
        REPARTITION,
        THROW,
    }
}