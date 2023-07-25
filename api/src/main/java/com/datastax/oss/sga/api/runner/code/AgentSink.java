package com.datastax.oss.sga.api.runner.code;

import java.util.List;

/**
 * Body of the agent
 */
public interface AgentSink extends AgentCode {

    /**
     * The agent processes records and typically writes then to an external service.
     * @param records the list of input records
     * @throws Exception if the agent fails to process the records
     */
    void write(List<Record> records) throws Exception;

    interface CommitCallback {
        void commit(List<Record> records);
    }
    
    void setCommitCallback(CommitCallback callback);

    /**
     * @return true if the agent handles commit of consumed record (e.g. in case of batching)
     */
    default boolean handlesCommit() {
        return false;
    }

    /** Let's sink handle offset commit if handlesCommit() == true.
     */
    default void commit() throws Exception {
    }

}
