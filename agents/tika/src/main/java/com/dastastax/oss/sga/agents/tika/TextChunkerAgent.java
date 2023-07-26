package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ParsingReader;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class TextChunkerAgent extends SingleRecordAgentFunction {

    /**
     * Number of characters per chunk.
     */
    private int chunkSize;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        chunkSize = Integer.parseInt(configuration.getOrDefault("chunkSize", "1000").toString());
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
    }
    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        Object value = record.value();
        final Reader stream = Utils.toReader(value);
        List<String> chunks = buildChunks(stream, chunkSize);
        return chunks
                .stream()
                .map( chunk -> SimpleRecord
                .builder()
                .key(record.key())
                .value(chunk)
                .origin(record.origin())
                .timestamp(record.timestamp())
                .headers(record.headers())
                .build())
                .collect(Collectors.toList());

    }

    public static List<String> buildChunks(Reader stream, int chunkSize) throws Exception {
        List<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int lastBreakingCharacterIndex = 0;
        int lastSpaceIndex = 0;
        int currentChunkSize = 0;
        for (int c = stream.read(); c != -1; c = stream.read()) {
            char character = (char) c;
            if (isBreakingCharacter(character)) {
                lastBreakingCharacterIndex = currentChunkSize;
            }
            if (isSpace(character)) {
                lastSpaceIndex = currentChunkSize;
            }
            sb.append(character);
            currentChunkSize++;
            if (currentChunkSize >= chunkSize) {
                if (lastBreakingCharacterIndex > 0 || lastSpaceIndex > 0) {
                    int bestBreakingIndex = lastBreakingCharacterIndex > 0 ? lastBreakingCharacterIndex : lastSpaceIndex;
                    // try to chunk at the last breaking character
                    result.add(sb.substring(0, bestBreakingIndex + 1));
                    sb = new StringBuilder(sb.substring(bestBreakingIndex + 1));
                    currentChunkSize = sb.length();
                } else {
                    result.add(sb.toString());
                    sb.setLength(0);
                    currentChunkSize = 0;
                }
                lastBreakingCharacterIndex = 0;
                lastSpaceIndex = 0;
            }
        }
        if (!sb.isEmpty()) {
            result.add(sb.toString());
        }
        return result;
    }

    private static boolean isBreakingCharacter(char character) {
        switch (character) {
            case '.':
            case ',':
            case '?':
            case ':':
                return true;
            default:
                return false;
        }
    }
    private static boolean isSpace(char character) {
        switch (character) {
            case ' ':
            case '\t':
            case '\n':
                return true;
            default:
                return false;
        }
    }
}
