package com.datastax.oss.sga.agents.tika;

import com.dastastax.oss.sga.agents.tika.TikaAgentsCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class LanguageDetectorTest {

    @Test
    public void textDetect() throws Exception {
        TikaAgentsCodeProvider provider = new TikaAgentsCodeProvider();
        SingleRecordAgentFunction instance = provider.createInstance("language-detector");
        instance.init(Map.of("property", "detected-language"));


        assertEquals("en", detectLanguage(instance, "This is a English"));
        assertEquals("it", detectLanguage(instance, "Questo é italiano"));
        assertEquals("fr", detectLanguage(instance, "Parlez-vous français?"));

    }

    @NotNull
    private static String detectLanguage(SingleRecordAgentFunction instance, String text) throws Exception {
        Record fromSource = SimpleRecord
                .builder()
                .key("filename.txt")
                .value(text.getBytes(StandardCharsets.UTF_8))
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);
        return result.getHeader("detected-language").value().toString();
    }

}
