package com.datastax.oss.sga.api.runner.code;

import java.util.List;
import java.util.Map;

/**
 * Body of the agent
 */
public interface AgentCode extends AutoCloseable {

    /**
     * The agent processes one record and returns a list of records.
     * The transactionality of the function is guaranteed by the runtime.
     * @param record
     * @return the list of records
     */
    List<Record> process(List<Record> record);

    default void init(Map<String, Object> configuration) {
    }

    default void start() {}
    default void close() {}
}
