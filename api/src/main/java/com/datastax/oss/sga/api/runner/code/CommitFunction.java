package com.datastax.oss.sga.api.runner.code;

import java.util.List;

public interface CommitFunction {
    void commit(List<Record> record) throws Exception;
}
