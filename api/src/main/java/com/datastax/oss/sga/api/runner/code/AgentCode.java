package com.datastax.oss.sga.api.runner.code;

import java.util.List;
import java.util.Map;

/**
 * Body of the agent
 */
public interface AgentCode {

    /**
     * The agent processes records and returns a list of records.
     * The transactionality of the function is guaranteed by the runtime.
     * @param records the list of input records
     * @return the list of output records
     * @throws Exception if the agent fails to process the records
     */
    List<Record> process(List<Record> records) throws Exception;

    default void init(Map<String, Object> configuration) throws Exception {
    }

    default void start() throws Exception {}
    default void close() throws Exception {}
}
