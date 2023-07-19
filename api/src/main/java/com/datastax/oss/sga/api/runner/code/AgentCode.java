package com.datastax.oss.sga.api.runner.code;

import java.util.List;
import java.util.Map;

/**
 * Body of the agent
 */
public interface AgentCode {

    default void init(Map<String, Object> configuration) throws Exception {
    }

    default void setContext(AgentContext context) throws Exception {
    }

    default void start() throws Exception {}
    default void close() throws Exception {}
}
