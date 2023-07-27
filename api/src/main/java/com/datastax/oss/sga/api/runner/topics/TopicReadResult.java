package com.datastax.oss.sga.api.runner.topics;

import com.datastax.oss.sga.api.runner.code.Record;
import java.util.List;
import java.util.Map;

public interface TopicReadResult {

    List<Record> records();

    OffsetPerPartition partitionsOffsets();
}
