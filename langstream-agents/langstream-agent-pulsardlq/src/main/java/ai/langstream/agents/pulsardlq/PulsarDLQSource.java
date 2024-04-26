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
package ai.langstream.agents.pulsardlq;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;

@Slf4j
public class PulsarDLQSource extends AbstractAgentCode implements AgentSource {
    private String pulsarUrl;
    private String namespace;
    private String subscription;
    private String dlqSuffix;
    private PulsarClient pulsarClient;
    private Consumer<byte[]> dlqTopicsConsumer;
    private boolean includePartitioned;

    private static class SimpleHeader implements Header {
        private final String key;
        private final Object value;

        public SimpleHeader(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String valueAsString() {
            if (value != null) {
                return value.toString();
            } else {
                return null;
            }
        }
    }

    private static class PulsarRecord implements Record {
        private final Message<byte[]> message;

        public PulsarRecord(Message<byte[]> message) {
            this.message = message;
        }

        @Override
        public Object key() {
            return message.getKey();
        }

        @Override
        public Object value() {
            return message.getData();
        }

        public MessageId messageId() {
            return message.getMessageId();
        }

        @Override
        public String origin() {
            return message.getTopicName(); // Static or derived from message properties
        }

        @Override
        public Long timestamp() {
            return message.getPublishTime(); // Using publish time as timestamp
        }

        @Override
        public Collection<Header> headers() {
            Collection<Header> headers = new ArrayList<>();
            Map<String, String> properties = message.getProperties();

            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    headers.add(new SimpleHeader(entry.getKey(), entry.getValue()));
                }
            }
            return headers;
        }

        @Override
        public String toString() {
            return "PulsarRecord{"
                    + "key="
                    + key()
                    + ", value="
                    + value()
                    + ", headers="
                    + headers()
                    + '}';
        }
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        pulsarUrl = ConfigurationUtils.getString("pulsar-url", "", configuration);
        namespace = ConfigurationUtils.getString("namespace", "", configuration);
        subscription = ConfigurationUtils.getString("subscription", "", configuration);
        dlqSuffix = ConfigurationUtils.getString("dlq-suffix", "-DLQ", configuration);
        includePartitioned =
                ConfigurationUtils.getBoolean("include-partitioned", false, configuration);
        log.info("Initializing PulsarDLQSource with pulsarUrl: {}", pulsarUrl);
        log.info("Namespace: {}", namespace);
        log.info("Subscription: {}", subscription);
        log.info("DLQ Suffix: {}", dlqSuffix);
        log.info("Include Partitioned: {}", includePartitioned);
    }

    @Override
    public void start() throws Exception {
        pulsarClient = PulsarClient.builder().serviceUrl(pulsarUrl).build();
        // The maximum lenth of the regex is 50 characters
        // Uisng the persistent:// prefix generally works better, but
        // it can push the partitioned pattern over the 50 character limit, so
        // we drop it for partitioned topics
        String patternString = "persistent://" + namespace + "/.*" + dlqSuffix;
        if (includePartitioned) {
            patternString = namespace + "/.*" + dlqSuffix + "(-p.+-\\d+)?";
        }

        Pattern dlqTopicsInNamespace = Pattern.compile(patternString);

        dlqTopicsConsumer =
                pulsarClient
                        .newConsumer()
                        .topicsPattern(dlqTopicsInNamespace)
                        .subscriptionName(subscription)
                        .subscribe();
    }

    @Override
    public void close() throws Exception {
        dlqTopicsConsumer.close();
        pulsarClient.close();
    }

    @Override
    public List<Record> read() throws Exception {

        Message<byte[]> msg = dlqTopicsConsumer.receive();

        log.info("Received message: {}", new String(msg.getData()));
        Record record = new PulsarRecord(msg);
        return List.of(record);
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        for (Record r : records) {
            PulsarRecord record = (PulsarRecord) r;
            dlqTopicsConsumer.acknowledge(record.messageId()); // acknowledge the message
        }
    }

    @Override
    public void permanentFailure(Record record, Exception error) throws Exception {
        PulsarRecord pulsarRecord = (PulsarRecord) record;
        log.error("Failure on record {}", pulsarRecord, error);
        dlqTopicsConsumer.negativeAcknowledge(pulsarRecord.messageId());
    }
}
