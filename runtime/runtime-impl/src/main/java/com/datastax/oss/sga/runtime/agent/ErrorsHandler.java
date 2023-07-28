package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.Record;
import net.bytebuddy.implementation.bytecode.Throw;

import java.util.List;

public interface ErrorsHandler {

    enum ErrorsProcessingOutcome {
        SKIP,
        RETRY,
        FAIL
    }

    ErrorsProcessingOutcome handleErrors(Record sourceRecord, Throwable error);
}
