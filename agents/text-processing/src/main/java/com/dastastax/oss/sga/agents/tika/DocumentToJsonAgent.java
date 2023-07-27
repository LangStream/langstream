package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j
public class DocumentToJsonAgent extends SingleRecordAgentFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String textField;
    private boolean copyProperties;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        this.textField = configuration.getOrDefault("text-field", "text").toString();
        this.copyProperties = Boolean.parseBoolean(configuration.getOrDefault("copy-properties", "true").toString());
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

        Map<String, Object> asJson = new HashMap<>();
        asJson.put(textField, stream);
        if (copyProperties) {
            record.headers().forEach(h -> asJson.put(h.key(), h.value()));
        }

        return List.of(SimpleRecord
                .copyFrom(record)
                .value(MAPPER.writeValueAsString(asJson))
                .build());

    }
}
