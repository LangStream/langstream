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
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class LanguageDetectorTest {

    @Test
    public void textDetect() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("language-detector");
        instance.init(Map.of("property", "detected-language"));

        assertEquals("en", detectLanguage(instance, "This is a English"));
        assertEquals("it", detectLanguage(instance, "Questo é italiano"));
        assertEquals("fr", detectLanguage(instance, "Parlez-vous français?"));
    }

    private static String detectLanguage(SingleRecordAgentProcessor instance, String text)
            throws Exception {
        Record fromSource =
                SimpleRecord.builder()
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
