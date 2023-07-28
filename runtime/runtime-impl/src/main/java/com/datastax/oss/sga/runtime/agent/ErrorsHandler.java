package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.Record;

import java.util.List;

public interface ErrorsHandler {

    enum ErrorsProcessingOutcome {
        SKIP,
        RETRY,
        FAIL
    }

    ErrorsProcessingOutcome handleErrors(List<Record> records, Exception error);
}
