package com.datastax.oss.sga.agents.tika;

import com.dastastax.oss.sga.agents.tika.TikaAgentsCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class TextExtractorTest {

    @Test
    public void textExtractFromText() throws Exception {
        TikaAgentsCodeProvider provider = new TikaAgentsCodeProvider();
        SingleRecordAgentFunction instance = provider.createInstance("text-extractor");

        Record fromSource = SimpleRecord
                .builder()
                .key("filename.txt")
                .value("This is a test".getBytes(StandardCharsets.UTF_8))
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);

    }
}
