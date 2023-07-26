package com.dastastax.oss.sga.agents.tika;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.StringReader;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TextChunkerAgentTest {

    @ParameterizedTest
    @MethodSource("testChunks")
    public void testChunks(int chunkSize, String text, List<String> expected) throws Exception {
        List<String> chunks = TextChunkerAgent.buildChunks(new StringReader(text), chunkSize);
        assertEquals(expected.size(), chunks.size(), "Bad number of chunks: "+  expected + " vs " + chunks);
        int i = 0;
        for (String expectedChunk : expected) {
            assertEquals(expectedChunk, chunks.get(i++));
        }
    }


    public static Stream<Arguments> testChunks() {
        return Stream.of(
                Arguments.of(20, "Hello world", List.of("Hello world")),
                Arguments.of(15, "Hello world. This is a great day", List.of("Hello world.", " This is a ", "great day")),
                Arguments.of(20, "", List.of()),
                Arguments.of(20, " ", List.of(" ")),
                Arguments.of(25, "This is a very long text, please split me", List.of("This is a very long text,"," please split me"))
        );
    }

}