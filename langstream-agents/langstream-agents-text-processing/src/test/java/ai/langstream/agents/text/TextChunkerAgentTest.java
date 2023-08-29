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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
class TextChunkerAgentTest {

    @ParameterizedTest
    @MethodSource("testChunks")
    public void testChunks(
            int chunkSize,
            int chunkOverlap,
            String text,
            String length_function,
            List<String> expected)
            throws Exception {

        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-splitter");
        instance.init(
                Map.of(
                        "splitter_type",
                        "RecursiveCharacterTextSplitter",
                        "separators",
                        List.of("\n\n", "\n", " ", ""),
                        "keep_separator",
                        false,
                        "chunk_size",
                        chunkSize,
                        "chunk_overlap",
                        chunkOverlap,
                        "length_function",
                        length_function));

        List<String> chunks = doChunking(instance, text);
        assertEquals(
                expected.size(),
                chunks.size(),
                "Bad number of chunks: " + expected + " vs " + chunks);
        int i = 0;
        for (String expectedChunk : expected) {
            assertEquals(expectedChunk, chunks.get(i++));
        }
    }

    private static List<String> doChunking(SingleRecordAgentProcessor instance, String text)
            throws Exception {
        Record fromSource =
                SimpleRecord.builder()
                        .key("filename.txt")
                        .value(text.getBytes(StandardCharsets.UTF_8))
                        .origin("origin")
                        .timestamp(System.currentTimeMillis())
                        .build();

        return instance.processRecord(fromSource).stream().map(r -> r.value().toString()).toList();
    }

    public static Stream<Arguments> testChunks() {
        return Stream.of(
                Arguments.of(20, 5, "Hello world", "length", List.of("Hello world")),
                Arguments.of(
                        15,
                        5,
                        "Hello world. This is a great day",
                        "length",
                        List.of("Hello world.", "This is a great", "great day")),
                Arguments.of(20, 5, "", "length", List.of()),
                Arguments.of(20, 5, " ", "length", List.of()),
                Arguments.of(20, 5, "Hello world", "cl100k_base", List.of("Hello world")),
                Arguments.of(
                        10,
                        2,
                        "Hello world, I would like to see some overlap here",
                        "cl100k_base",
                        List.of(
                                "Hello world, I would like",
                                "like to see some overlap",
                                "overlap here")));
    }

    @Test
    public void testChunksKeepSeparator() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-splitter");
        instance.init(
                Map.of(
                        "splitter_type",
                        "RecursiveCharacterTextSplitter",
                        "separators",
                        List.of("\n\n", "\n", " ", ""),
                        "keep_separator",
                        true,
                        "chunk_size",
                        15,
                        "chunk_overlap",
                        5,
                        "length_function",
                        "length"));

        List<String> chunks = doChunking(instance, "Hello world. This is a great day");
        List<String> expected = List.of("Hello world.", "This is a", "is a great day");
        assertEquals(
                expected.size(),
                chunks.size(),
                "Bad number of chunks: " + expected + " vs " + chunks);
        int i = 0;
        for (String expectedChunk : expected) {
            assertEquals(expectedChunk, chunks.get(i++));
        }
    }

    @Test
    public void testChunksRegexSeparator() throws Exception {
        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentProcessor instance = provider.createInstance("text-splitter");
        instance.init(
                Map.of(
                        "splitter_type",
                        "RecursiveCharacterTextSplitter",
                        "separators",
                        List.of("\\d+"),
                        "keep_separator",
                        true,
                        "chunk_size",
                        15,
                        "chunk_overlap",
                        5,
                        "length_function",
                        "length"));

        List<String> chunks = doChunking(instance, "Hello1world.2This3is4a5great6day");
        List<String> expected = List.of("Hello1world.", "2This3is4a", "3is4a5great6day");
        assertEquals(
                expected.size(),
                chunks.size(),
                "Bad number of chunks: " + expected + " vs " + chunks);
        int i = 0;
        for (String expectedChunk : expected) {
            assertEquals(expectedChunk, chunks.get(i++));
        }
    }
}
