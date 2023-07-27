package com.datastax.oss.sga.api.runner.topics;

public record TopicOffsetPosition(Position position, String offset) {

    public enum Position {
        Latest,
        Earliest,
        Absolute;
    }

    public static final TopicOffsetPosition LATEST = new TopicOffsetPosition(Position.Latest, null);
    public static final TopicOffsetPosition EARLIEST = new TopicOffsetPosition(Position.Earliest, null);
    public static TopicOffsetPosition absolute(String offset) {
        return new TopicOffsetPosition(Position.Absolute, offset);
    }

}
