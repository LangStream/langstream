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

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ParsingReader;

@Slf4j
public class TikaTextExtractorAgent extends SingleRecordAgentProcessor {

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        AutoDetectParser parser = new AutoDetectParser();
        Object value = record.value();
        final InputStream stream = Utils.toStream(value);
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        try (Reader reader = new ParsingReader(parser, stream, metadata, parseContext)) {
            StringWriter valueAsString = new StringWriter();
            String[] names = metadata.names();
            log.info(
                    "Document type: {} Content {}",
                    Stream.of(names).collect(Collectors.toMap(Function.identity(), metadata::get)),
                    valueAsString);
            reader.transferTo(valueAsString);
            return List.of(SimpleRecord.copyFrom(record).value(valueAsString.toString()).build());
        }
    }
}
