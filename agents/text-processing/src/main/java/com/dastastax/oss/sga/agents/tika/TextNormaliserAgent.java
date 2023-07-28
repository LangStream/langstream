package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j
public class TextNormaliserAgent extends SingleRecordAgentProcessor {

    private boolean makeLowercase = true;
    private boolean trimSpaces = true;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        makeLowercase = Boolean.parseBoolean(configuration.getOrDefault("make-lowercase", "true").toString());
        trimSpaces = Boolean.parseBoolean(configuration.getOrDefault("trim-spaces", "true").toString());
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
    }
    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        Object value = record.value();
        String stream = Utils.toText(value);

        if (makeLowercase) {
            stream = stream.toLowerCase(Locale.ENGLISH);
        }
        if (trimSpaces) {
            stream = stream.trim();
        }
        return List.of(SimpleRecord
                .builder()
                .value(stream)
                .build());

    }
}
