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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultProducer;

@Slf4j
public class CamelSource extends AbstractAgentCode implements AgentSource {

    private String componentUri;
    private DefaultCamelContext camelContext;

    private ArrayBlockingQueue<SimpleRecord> records;

    private DefaultEndpoint endpoint;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        componentUri = ConfigurationUtils.getString("component-uri", "", configuration);
        int maxRecords = ConfigurationUtils.getInt("max-records", 100, configuration);
        log.info("Initializing CamelSource with componentUri: {}", componentUri);
        log.info("Max pending records: {}", maxRecords);

        records = new ArrayBlockingQueue<>(maxRecords);
    }

    @Override
    public void start() throws Exception {
        camelContext = new DefaultCamelContext();

        endpoint =
                new DefaultEndpoint() {
                    @Override
                    public boolean isSingleton() {
                        return true;
                    }

                    @Override
                    public Producer createProducer() throws Exception {
                        return new DefaultProducer(this) {

                            @Override
                            public void process(Exchange exchange) throws Exception {
                                log.info("Processing exchange: {}", exchange);
                                Message in = exchange.getIn();
                                SimpleRecord.SimpleRecordBuilder recordBuilder =
                                        SimpleRecord.builder();
                                Collection<Header> headers = new ArrayList<>();
                                if (in.hasHeaders()) {
                                    in.getHeaders()
                                            .forEach(
                                                    (k, v) ->
                                                            headers.add(
                                                                    new SimpleRecord.SimpleHeader(
                                                                            k, v)));
                                }
                                recordBuilder.headers(headers);
                                recordBuilder.value(in.getBody());
                                recordBuilder.timestamp(in.getMessageTimestamp());
                                recordBuilder.origin(componentUri);
                                SimpleRecord build = recordBuilder.build();
                                log.info("Created record {}", build);
                                records.add(build);
                            }
                        };
                    }

                    @Override
                    public CamelContext getCamelContext() {
                        return camelContext;
                    }

                    @Override
                    public Consumer createConsumer(Processor processor) throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getEndpointUri() {
                        return "langstream://camel-source";
                    }

                    @Override
                    public boolean isSingletonProducer() {
                        return true;
                    }
                };

        camelContext.addRoutes(
                new RouteBuilder() {
                    @Override
                    public void configure() throws Exception {
                        from(componentUri).to(endpoint);
                    }
                });

        camelContext.start();
    }

    @Override
    public void close() throws Exception {
        if (endpoint != null) {
            endpoint.close();
        }
        if (camelContext != null) {
            camelContext.close();
        }
    }

    @Override
    public List<Record> read() throws Exception {
        SimpleRecord poll = records.poll(1, TimeUnit.SECONDS);
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
    public void commit(List<Record> records) throws Exception {}
}
