package com.datastax.oss.sga.api.runner.code;

import java.util.List;

/**
 * Body of the agent
 */
public interface AgentSource extends AgentCode {

    /**
     * The agent generates records returns them as list of records.
     * @return the list of output records
     * @throws Exception if the agent fails to process the records
     */
    List<Record> read() throws Exception;


    /**
     * Called by the framework to indicate that the agent has successfully processed
     * all the records returned by read up to the latest.
     */
    void commit(List<Record> record) throws Exception;
}
