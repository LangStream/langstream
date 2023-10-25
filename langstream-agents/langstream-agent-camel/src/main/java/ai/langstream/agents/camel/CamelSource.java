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
package ai.langstream.agents.camel;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.util.ConfigurationUtils;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.AsyncProcessorSupport;
import org.jetbrains.annotations.Nullable;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class CamelSource extends AbstractAgentCode implements AgentSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String componentUri;
    private DefaultCamelContext camelContext;

    private ArrayBlockingQueue<CamelRecord> records;

    private String keyHeader;

    private static class CamelRecord implements Record {
        private final AsyncCallback callback;
        private final Exchange exchange;
        private final Object key;
        private final Object value;
        private final Collection<Header> headers;
        private final String origin;
        private final long timestamp;

        public CamelRecord(
                AsyncCallback callback,
                Exchange exchange,
                Object key,
                Object value,
                String origin,
                long timestamp,
                Collection<Header> headers) {
            this.callback = callback;
            this.exchange = exchange;
            this.key = key;
            this.value = value;
            this.headers = headers;
            this.origin = origin;
            this.timestamp = timestamp;
        }

        @Override
        public Object key() {
            return key;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String origin() {
            return origin;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }

        @Override
        public Collection<Header> headers() {
            return headers;
        }

        @Override
        public String toString() {
            return "CamelRecord{"
                    + "key="
                    + key
                    + ", value="
                    + value
                    + ", headers="
                    + headers
                    + '}';
        }
    }

    private class LangStreamProcessor extends AsyncProcessorSupport {

        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {

            log.info("Processing exchange: {}", exchange);
            Message in = exchange.getIn();
            try {
                Collection<Header> headers = new ArrayList<>();
                if (in.hasHeaders()) {
                    for (Map.Entry<String, Object> header : in.getHeaders().entrySet()) {
                        String k = header.getKey();
                        Object v = header.getValue();
                        Object converted = safeObject(v);
                        headers.add(new SimpleRecord.SimpleHeader(k, converted));
                    }
                    ;
                }
                Object key = keyHeader.isEmpty() ? null : safeObject(in.getHeader(keyHeader));
                CamelRecord record =
                        new CamelRecord(
                                callback,
                                exchange,
                                key,
                                safeObject(in.getBody()),
                                componentUri,
                                in.getMessageTimestamp(),
                                headers);
                log.info("Created record {}", record);
                records.add(record);
            } catch (Exception error) {
                log.error("Error serializing record", error);
                exchange.setException(error);
                callback.done(true);
            }

            return true;
        }

        @Nullable
        private static Object safeObject(Object v) throws JsonProcessingException {
            Object converted;
            if (v == null) {
                converted = null;
            } else if (v instanceof CharSequence || v instanceof Number || v instanceof Boolean) {
                converted = v;
            } else {
                converted = MAPPER.writeValueAsString(v);
            }
            return converted;
        }
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        componentUri = ConfigurationUtils.getString("component-uri", "", configuration);
        int maxRecords = ConfigurationUtils.getInt("max-buffered-records", 100, configuration);
        this.keyHeader = ConfigurationUtils.getString("key-header", "", configuration);
        Map<String, Object> componentOptions =
                ConfigurationUtils.getMap("component-options", Map.of(), configuration);
        if (componentOptions != null) {
            for (Map.Entry<String, Object> entry : componentOptions.entrySet()) {
                String paramName = entry.getKey();
                Object value = entry.getValue();
                if (value != null) {
                    if (componentUri.contains("?")) {
                        componentUri += "&";
                    } else {
                        componentUri += "?";
                    }
                    componentUri += paramName + "=" + URLEncoder.encode(value.toString(), "UTF-8");
                }
            }
        }
        log.info("Initializing CamelSource with componentUri: {}", componentUri);
        log.info("Max pending records: {}", maxRecords);
        log.info("Key header: {}", keyHeader);

        records = new ArrayBlockingQueue<>(maxRecords);
    }

    @Override
    public void start() throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.addRoutes(
                new RouteBuilder() {
                    @Override
                    public void configure() throws Exception {
                        from(componentUri).process(new LangStreamProcessor());
                    }
                });

        camelContext.start();
    }

    @Override
    public void close() throws Exception {
        if (camelContext != null) {
            camelContext.close();
        }
    }

    @Override
    public List<Record> read() throws Exception {
        CamelRecord poll = records.poll(1, TimeUnit.SECONDS);
        if (poll != null) {
            processed(0, 1);
            return List.of(poll);
        } else {
            return List.of();
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("component-uri", componentUri);
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        for (Record r : records) {
            CamelRecord camelRecord = (CamelRecord) r;
            camelRecord.callback.done(true);
        }
    }

    @Override
    public void permanentFailure(Record record, Exception error) throws Exception {
        CamelRecord camelRecord = (CamelRecord) record;
        log.info("Record {} failed", camelRecord);
        camelRecord.exchange.setException(error);
    }
}
