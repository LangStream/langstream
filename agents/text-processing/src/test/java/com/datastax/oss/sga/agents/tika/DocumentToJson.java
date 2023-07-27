package com.datastax.oss.sga.agents.tika;

import com.dastastax.oss.sga.agents.tika.TextProcessingAgentsCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class DocumentToJson {

    @Test
    public void textConvertToJson() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentFunction instance = provider.createInstance("document-to-json");
        instance.init(Map.of("text-field", "document", "copy-properties", "true"));

        assertEquals("{\"detected-language\":\"en\",\"document\":\"This is a English\"}",
                convertToJson(instance, "This is a English"));

        instance.init(Map.of("text-field", "document", "copy-properties", "false"));
        assertEquals("{\"document\":\"This is a English\"}", convertToJson(instance, "This is a English"));

    }

    private static String convertToJson(SingleRecordAgentFunction instance, String text) throws Exception {
        Record fromSource = SimpleRecord
                .builder()
                .key("filename.txt")
                .value(text.getBytes(StandardCharsets.UTF_8))
                .origin("origin")
                .headers(List.of(new SimpleRecord.SimpleHeader("detected-language", "en")))
                .timestamp(System.currentTimeMillis())
                .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);
        return (String) result.value();
    }

}
