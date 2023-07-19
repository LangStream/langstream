package com.datastax.oss.sga.api.runner.code;

import java.util.List;

/**
 * Body of the agent
 */
public interface AgentSink extends AgentCode {

    /**
     * The agent processes records and tyipically writes then to an external service.
     * @param records the list of input records
     * @throws Exception if the agent fails to process the records
     */
    void write(List<Record> records) throws Exception;
}
