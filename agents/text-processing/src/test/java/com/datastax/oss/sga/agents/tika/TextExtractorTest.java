package com.datastax.oss.sga.agents.tika;

import com.dastastax.oss.sga.agents.tika.TextProcessingAgentsCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class TextExtractorTest {

    @Test
    public void textExtractFromText() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-extractor");

        Record fromSource = SimpleRecord
                .builder()
                .key("filename.txt")
                .value("This is a test".getBytes(StandardCharsets.UTF_8))
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);
        assertEquals("This is a test", result.value().toString().trim());

    }

    @Test
    public void textExtractFromPdf() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-extractor");

        byte[] content = Files.readAllBytes(Paths.get("src/test/resources/simple.pdf"));

        Record fromSource = SimpleRecord
                .builder()
                .key("filename.pdf")
                .value(content)
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);

        assertEquals("This is a very simple PDF", result.value().toString().trim());

    }

    @Test
    public void textExtractFromWord() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-extractor");

        byte[] content = Files.readAllBytes(Paths.get("src/test/resources/simple.docx"));

        Record fromSource = SimpleRecord
                .builder()
                .key("filename.doc")
                .value(content)
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);

        assertEquals("This is a very simple Word Document", result.value().toString().trim());

    }
}
