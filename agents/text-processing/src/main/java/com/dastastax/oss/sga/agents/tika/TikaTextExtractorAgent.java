package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ParsingReader;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class TikaTextExtractorAgent extends SingleRecordAgentProcessor {

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
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
        AutoDetectParser parser = new AutoDetectParser();
        Object value = record.value();
        final InputStream stream = Utils.toStream(value);
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        Reader reader = new ParsingReader(parser, stream, metadata, parseContext);
        try {
            StringWriter valueAsString = new StringWriter();
            String[] names = metadata.names();
            log.info("Document type: {} Content {}", Stream.of(names)
                    .collect(Collectors.toMap(Function.identity(), metadata::get)), valueAsString);
            reader.transferTo(valueAsString);
            return List.of(SimpleRecord
                    .builder()
                    .key(record.key())
                    .value(valueAsString.toString())
                    .origin(record.origin())
                    .timestamp(record.timestamp())
                    .headers(record.headers())
                    .build());
        } finally {
            reader.close();
        }
    }
}
