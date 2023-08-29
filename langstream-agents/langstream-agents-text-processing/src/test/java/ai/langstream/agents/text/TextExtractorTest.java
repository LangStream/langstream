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
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class TextExtractorTest {

    @Test
    public void textExtractFromText() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-extractor");

        Record fromSource =
                SimpleRecord.builder()
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

        Record fromSource =
                SimpleRecord.builder()
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

        Record fromSource =
                SimpleRecord.builder()
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
