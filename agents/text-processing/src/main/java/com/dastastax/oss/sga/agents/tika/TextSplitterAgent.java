/**
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
package com.dastastax.oss.sga.agents.tika;


import java.util.ArrayList;
import java.util.List;
import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.stream.Collectors;


/**
 * Perform text splitting tasks.
 */
@Slf4j
public class TextSplitterAgent extends SingleRecordAgentProcessor {

    /**
     * Number of characters per chunk.
     */
    private TextSplitter textSplitter;
    private LengthFunction lengthFunction;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        initTextSplitter(configuration);
    }

    private void initTextSplitter(Map<String, Object> configuration) {
        String splitterType = configuration.getOrDefault("splitter_type",
                "RecursiveCharacterTextSplitter").toString();
        TextSplitter newTextSplitter;
        LengthFunction newLengthFunction;
        switch (splitterType) {
            case "RecursiveCharacterTextSplitter":
                List<String> separators = (List<String>) configuration.getOrDefault("separators",
                        List.of("\n\n", "\n", " ", ""));
                boolean keepSeparator = Boolean.parseBoolean(configuration.getOrDefault("keep_separator", "false").toString());
                int chunkSize = Integer.parseInt(configuration.getOrDefault("chunk_size", "200").toString());
                int chunkOverlap = Integer.parseInt(configuration.getOrDefault("chunk_overlap", "100").toString());
                String lengthFunctionName = configuration.getOrDefault("length_function", "cl100k_base").toString();
                switch (lengthFunctionName) {
                    case "length":
                        newLengthFunction = String::length;
                        break;
                    default:
                        newLengthFunction = new TikTokLengthFunction(lengthFunctionName);
                }
                newTextSplitter = new RecursiveCharacterTextSplitter(separators, keepSeparator, chunkSize, chunkOverlap,  newLengthFunction::length);
                break;
            default:
                throw new IllegalArgumentException("Unknown splitter type: " + splitterType +", only RecursiveCharacterTextSplitter is supported");
        }
        this.textSplitter = newTextSplitter;
        this.lengthFunction = newLengthFunction;
    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        Object value = record.value();
        String text = Utils.toText(value);
        List<String> chunks = textSplitter.splitText(text);
        int chunkId = 0;
        List<Record> result = new ArrayList<>();
        for (String chunk : chunks) {
            List<Header> chunkHeaders = new ArrayList<>(record.headers());
            chunkHeaders.add(new SimpleRecord.SimpleHeader("chunk_id", String.valueOf(chunkId++)));
            chunkHeaders.add(new SimpleRecord.SimpleHeader("chunk_text_length", String.valueOf(chunk.length())));
            int numTokens = lengthFunction.length(chunk);
            chunkHeaders.add(new SimpleRecord.SimpleHeader("chunk_num_tokens", String.valueOf(numTokens)));

            // here we are setting as key the original key
            // this is to ensure that all the chunks will be processed in order downstream
            // it is important to not enable compaction on this topic
            SimpleRecord chunkRecord = SimpleRecord
                    .copyFrom(record)
                    .key(record.key())
                    .value(chunk)
                    .headers(chunkHeaders)
                    .build();
            result.add(chunkRecord);
        }
        return result;
    }



}
