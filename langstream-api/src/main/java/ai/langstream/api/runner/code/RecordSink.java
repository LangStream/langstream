package ai.langstream.api.runner.code;

public interface RecordSink {
    /**
     * Emit a record to the downstream agent.
     * This method can be called by any thread
     * and it is guaranteed to not throw any exception.
     * @param recordAndResult the result of the agent processing
     */
    void emit(AgentProcessor.SourceRecordAndResult recordAndResult);

}
