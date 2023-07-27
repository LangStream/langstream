package com.dastastax.oss.sga.agents.tika;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;



@Slf4j
public class TextChunkerAgent extends SingleRecordAgentFunction {

    /**
     * Number of characters per chunk.
     */
    private TextSplitter textSplitter;


    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        textSplitter = initTextSplitter(configuration);
    }

    private static TextSplitter initTextSplitter(Map<String, Object> configuration) {
        String splitterType = configuration.getOrDefault("splitter_type",
                "RecursiveCharacterTextSplitter").toString();
        switch (splitterType) {
            case "RecursiveCharacterTextSplitter":
                List<String> separators = (List<String>) configuration.getOrDefault("separators",
                        List.of("\n\n", "\n", " ", ""));
                boolean keepSeparator = Boolean.parseBoolean(configuration.getOrDefault("keep_separator", "false").toString());
                int chunkSize = Integer.parseInt(configuration.getOrDefault("chunk_size", "200").toString());
                int chunkOverlap = Integer.parseInt(configuration.getOrDefault("chunk_overlap", "100").toString());
                LengthFunction lengthFunction;
                String lengthFunctionName = configuration.getOrDefault("length_function", "cl100k_base").toString();
                switch (lengthFunctionName) {
                    case "length":
                        lengthFunction = String::length;
                        break;
                    default:
                        lengthFunction = new TikTokLengthFunction(lengthFunctionName);
                }
                return new RecursiveCharacterTextSplitter(separators, keepSeparator, chunkSize, chunkOverlap,  lengthFunction::length);
            default:
                throw new IllegalArgumentException("Unknown splitter type: " + splitterType +", only RecursiveCharacterTextSplitter is supported");
        }
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
        String text = Utils.toText(value);
        List<String> chunks = textSplitter.splitText(text);
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



}
