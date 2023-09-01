/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.agents.text;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class DocumentToJsonTest {

    @Test
    public void textConvertToJson() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("document-to-json");
        instance.init(Map.of("text-field", "document", "copy-properties", "true"));

        assertEquals(
                "{\"detected-language\":\"en\",\"document\":\"This is a English\",\"other-header\":\"bytearray-value\"}",
                convertToJson(instance, "This is a English"));

        instance.init(Map.of("text-field", "document", "copy-properties", "false"));
        assertEquals(
                "{\"document\":\"This is a English\"}",
                convertToJson(instance, "This is a English"));
    }

    @Test
    public void textConvertToJsonWithRawHeaders() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("document-to-json");
        instance.init(Map.of("text-field", "document", "copy-properties", "true"));

        assertEquals(
                "{\"detected-language\":\"en\",\"document\":\"This is a English\",\"other-header\":\"bytearray-value\"}",
                convertToJson(instance, "This is a English"));

        instance.init(Map.of("text-field", "document", "copy-properties", "false"));
        assertEquals(
                "{\"document\":\"This is a English\"}",
                convertToJson(instance, "This is a English"));
    }

    private static String convertToJson(SingleRecordAgentProcessor instance, String text)
            throws Exception {
        Record fromSource =
                SimpleRecord.builder()
                        .key("filename.txt")
                        .value(text.getBytes(StandardCharsets.UTF_8))
                        .origin("origin")
                        .headers(
                                List.of(
                                        new SimpleRecord.SimpleHeader("detected-language", "en"),
                                        new SimpleRecord.SimpleHeader(
                                                "other-header",
                                                "bytearray-value"
                                                        .getBytes(StandardCharsets.UTF_8))))
                        .timestamp(System.currentTimeMillis())
                        .build();

        Record result = instance.processRecord(fromSource).get(0);
        log.info("Result: {}", result);
        return (String) result.value();
    }
}
