package com.datastax.oss.sga.api.runner.code;

import java.util.List;
import java.util.Map;

/**
 * Body of the agent
 */
public interface AgentFunction extends AgentCode {

    /**
     * The agent processes records and returns a list of records.
     * The transactionality of the function is guaranteed by the runtime.
     * @param records the list of input records
     * @return the list of output records
     * @throws Exception if the agent fails to process the records
     */
    Map<Record, List<Record>> process(List<Record> records) throws Exception;
}
