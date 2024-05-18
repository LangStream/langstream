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

import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ParsingReader;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

@Slf4j
public class TikaTextExtractorAgent extends SingleRecordAgentProcessor {

    private class CustomContentHandler extends BodyContentHandler {
        private boolean hasTextContent = false;

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            super.characters(ch, start, length);
            hasTextContent = true;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts)
                throws SAXException {
            if (!"img".equalsIgnoreCase(localName)) {
                super.startElement(uri, localName, qName, atts);
            }
        }

        public boolean hasTextContent() {
            return hasTextContent;
        }
    }

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
        StringWriter valueAsString;

        CustomContentHandler handler = new CustomContentHandler();

        // ParsingReader starts new threads, so we need to make sure they are cleaned up
        try (Reader reader = new ParsingReader(parser, stream, metadata, parseContext)) {
            valueAsString = new StringWriter();
            reader.transferTo(valueAsString);
        }

        // If there are only images in the content, we don't want to return anything
        String content = valueAsString.toString();

        String[] names = metadata.names();
        Map<String, String> metadataMap =
                Arrays.stream(names)
                        .collect(
                                Collectors.toMap(
                                        Function.identity(),
                                        key -> String.join(", ", metadata.getValues(key))));

        if (log.isDebugEnabled()) {
            log.debug("Document type: {} Content {}", metadataMap, content);
        }

        // Create a new set of headers
        Set<Header> newHeaders = new HashSet<>(record.headers()); // Copy existing headers

        metadataMap.forEach(
                (key, metaValue) -> newHeaders.add(new SimpleRecord.SimpleHeader(key, metaValue)));

        // Add another header for content length
        newHeaders.add(
                new SimpleRecord.SimpleHeader("Content-Length", String.valueOf(content.length())));

        // Build the new record with the new set of headers
        SimpleRecord newRecord =
                SimpleRecord.copyFrom(record)
                        .value(content)
                        .headers(newHeaders) // Set the entire headers collection
                        .build();

        return List.of(newRecord);
    }
}
