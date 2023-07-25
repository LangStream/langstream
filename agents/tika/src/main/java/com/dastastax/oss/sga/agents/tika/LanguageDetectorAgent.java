package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.langdetect.tika.LanguageIdentifier;
import java.util.List;
import java.util.Map;

@Slf4j
public class LanguageDetectorAgent extends SingleRecordAgentFunction {

    private String field = "language";

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        if (configuration.containsKey("field")) {
            field = (String) configuration.get("field");
        }
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
        String inputText = Utils.toText(record.value());

        LanguageIdentifier identifier = new LanguageIdentifier(inputText);
        String language = identifier.getLanguage();

        Record result = SimpleRecord
                .copyFrom(record)
                .headers(Utils.addHeader(record.headers(), SimpleRecord.SimpleHeader.of(field, language)))
                .build();

        return List.of(result);
    }
}
