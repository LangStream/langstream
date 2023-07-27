package com.datastax.oss.sga.agents.tika;

import com.dastastax.oss.sga.agents.tika.TextProcessingAgentsCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class TextChunkerAgentTest {

    @ParameterizedTest
    @MethodSource("testChunks")
    public void testChunks(int chunkSize, int chunkOverlap, String text, String length_function, List<String> expected) throws Exception {

        TextProcessingAgentsCodeProvider provider = new TextProcessingAgentsCodeProvider();
        SingleRecordAgentFunction instance = provider.createInstance("text-splitter");
        instance.init(Map.of("splitter_type", "RecursiveCharacterTextSplitter",
        "separators", List.of("\n\n", "\n", " ", ""),
                "keep_separator", "true",
                "chunk_size", chunkSize,
                "chunk_overlap", chunkOverlap,
                "length_function", length_function));

        List<String> chunks = doChunking(instance, text);
        assertEquals(expected.size(), chunks.size(), "Bad number of chunks: "+  expected + " vs " + chunks);
        int i = 0;
        for (String expectedChunk : expected) {
            assertEquals(expectedChunk, chunks.get(i++));
        }
    }

    private static List<String> doChunking(SingleRecordAgentFunction instance, String text) throws Exception {
        Record fromSource = SimpleRecord
                .builder()
                .key("filename.txt")
                .value(text.getBytes(StandardCharsets.UTF_8))
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .build();

        return instance.processRecord(fromSource).stream().map(r -> r.value().toString()).toList();
    }


    public static Stream<Arguments> testChunks() {
        return Stream.of(
                Arguments.of(20, 5, "Hello world", "length", List.of("Hello   world"))
            /*    Arguments.of(15, 5, "Hello world. This is a great day", "length", List.of("Hello   world.", "This   is   a", "a   great", "day")),
                Arguments.of(20, 5, "", "length", List.of()),
                Arguments.of(20, 5, " ", "length", List.of(),
                Arguments.of(25, 5, "length", "This is a very long text, please split me", List.of("This   is   a   very"," long   text", "   please, ", " split   me"))),
                Arguments.of(20, 5, "Hello world", "cl100k_base", List.of("Hello   world")),
                Arguments.of(20, 10, "Hello world, I would like to see some overlap here", "cl100k_base", List.of("Hello   world,   I   would   like",
                        " would   like   to   see   some", " to   see   some   overlap   here")) */
        );
    }

}