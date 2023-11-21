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
package ai.langstream.agents.vector.cassandra;

import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.AbstractSinkRecordHeader;
import com.datastax.oss.common.sink.AbstractSinkTask;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.util.SinkUtil;
import com.datastax.oss.streaming.ai.datasource.CassandraDataSource;
import com.dtsx.astra.sdk.db.DbOpsClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraWriter implements VectorDatabaseWriterProvider {

    private static final String DUMMY_TOPIC = "langstreaminputtopic";

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "cassandra".equals(dataSourceConfig.get("service"))
                || "astra".equals(dataSourceConfig.get("service"));
    }

    @Override
    public VectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new CassandraVectorDatabaseWriter(datasourceConfig);
    }

    private static class CassandraVectorDatabaseWriter implements VectorDatabaseWriter {

        private final Map<String, Object> datasourceConfig;
        private final AbstractSinkTask processor = new SinkTaskProcessorImpl();

        public CassandraVectorDatabaseWriter(Map<String, Object> datasourceConfig) {
            log.debug(
                    "CassandraSinkTask starting with DataSource configuration: {}",
                    datasourceConfig);
            this.datasourceConfig = datasourceConfig;
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) {
            Map<String, String> configuration = new HashMap<>();
            agentConfiguration.forEach(
                    (k, v) -> {
                        if (v instanceof String || v instanceof Number || v instanceof Boolean) {
                            configuration.put(k, v.toString());
                        } else if (v == null) {
                            configuration.put(k, null);
                        } else {
                            switch (k) {
                                case "datasource":
                                    Map<String, Object> datasource = (Map<String, Object>) v;

                                    // this is the same format supported by the QueryStepDataSource
                                    // for Cassandra

                                    // Cassandra
                                    configuration.put(
                                            "contactPoints",
                                            ConfigurationUtils.getString(
                                                    "contact-points", "", datasource));
                                    configuration.put(
                                            "loadBalancing.localDc",
                                            ConfigurationUtils.getString(
                                                    "loadBalancing-localDc", "", datasource));
                                    configuration.put(
                                            "port",
                                            ConfigurationUtils.getString(
                                                    "port", "9042", datasource));

                                    String token =
                                            ConfigurationUtils.getString("token", "", datasource);
                                    String secureBundleString =
                                            ConfigurationUtils.getString(
                                                    "secureBundle", "", datasource);
                                    // AstraDB, explicit secureBundle
                                    if (!secureBundleString.isEmpty()) {
                                        configuration.put(
                                                "cloud.secureConnectBundle", secureBundleString);
                                    } else {
                                        // AstraDB, token/database/databaseId

                                        String database =
                                                ConfigurationUtils.getString(
                                                        "database", "", datasource);
                                        String databaseId =
                                                ConfigurationUtils.getString(
                                                        "database-id", "", datasource);
                                        String environment =
                                                ConfigurationUtils.getString(
                                                        "environment", "PROD", datasource);
                                        if (!token.isEmpty()
                                                && (!database.isEmpty() || !databaseId.isEmpty())) {
                                            DbOpsClient databaseClient =
                                                    CassandraDataSource.buildAstraClient(
                                                            token,
                                                            database,
                                                            databaseId,
                                                            environment);
                                            log.info(
                                                    "Automatically downloading the secure bundle from AstraDB");
                                            byte[] secureBundle =
                                                    CassandraDataSource.downloadSecureBundle(
                                                            databaseClient);
                                            configuration.put(
                                                    "cloud.secureConnectBundle",
                                                    "base64:"
                                                            + Base64.getEncoder()
                                                                    .encodeToString(secureBundle));
                                        }
                                    }

                                    configuration.put(
                                            "auth.username",
                                            ConfigurationUtils.getString(
                                                    "username",
                                                    ConfigurationUtils.getString(
                                                            "clientId", "token", datasource),
                                                    datasource));
                                    configuration.put(
                                            "auth.password",
                                            ConfigurationUtils.getString(
                                                    "password",
                                                    ConfigurationUtils.getString(
                                                            "secret", token, datasource),
                                                    datasource));

                                    break;
                                default:
                                    throw new IllegalArgumentException(
                                            "Only string values can be passed to the Cassandra sink, found "
                                                    + v.getClass()
                                                    + " for "
                                                    + k);
                            }
                        }
                    });
            configuration.put(SinkUtil.NAME_OPT, "langstream");
            String table =
                    ConfigurationUtils.getString(
                            "table",
                            ConfigurationUtils.getString("table-name", "", agentConfiguration),
                            agentConfiguration);
            String keyspace = (String) agentConfiguration.get("keyspace");
            if (keyspace != null && !keyspace.isEmpty()) {
                table = keyspace + "." + table;
            }
            String mapping = ConfigurationUtils.getString("mapping", "", agentConfiguration);
            configuration.put("topics", DUMMY_TOPIC);
            configuration.put("topic." + DUMMY_TOPIC + "." + table + ".mapping", mapping);

            processor.start(configuration);
        }

        private final Map<Record, CompletableFuture<?>> currentRecordStatus =
                new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {
            // we must handle one record at a time
            // so we block until the record is processed
            CompletableFuture<?> handle = new CompletableFuture();
            currentRecordStatus.put(record, handle);
            processor.put(List.of(new LangStreamSinkRecordAdapter(record)));
            return handle;
        }

        @Override
        public void close() {
            processor.stop();
        }

        private class SinkTaskProcessorImpl extends AbstractSinkTask {
            @Override
            public String version() {
                return "";
            }

            @Override
            public String applicationName() {
                return "langstream";
            }

            @Override
            protected void handleSuccess(AbstractSinkRecord abstractRecord) {
                Record record = ((LangStreamSinkRecordAdapter) abstractRecord).getRecord();
                CompletableFuture<?> remove = currentRecordStatus.remove(record);
                remove.complete(null);
            }

            @Override
            protected void handleFailure(
                    AbstractSinkRecord abstractRecord,
                    Throwable e,
                    String cql,
                    Runnable failCounter) {
                // Store the topic-partition and offset that had an error. However, we want
                // to keep track of the *lowest* offset in a topic-partition that failed. Because
                // requests are sent in parallel and response ordering is non-deterministic,
                // it's possible for a failure in an insert with a higher offset be detected
                // before that of a lower offset. Thus, we only record a failure if
                // 1. There is no entry for this topic-partition, or
                // 2. There is an entry, but its offset is > our offset.
                //
                // This can happen in multiple invocations of this callback concurrently, so
                // we perform these checks/updates in a synchronized block. Presumably failures
                // don't occur that often, so we don't have to be very fancy here.
                Record record = ((LangStreamSinkRecordAdapter) abstractRecord).getRecord();
                CassandraSinkConfig.IgnoreErrorsPolicy ignoreErrors =
                        processor.getInstanceState().getConfig().getIgnoreErrors();
                boolean driverFailure = cql != null;
                if (driverFailure) {
                    log.warn(
                            "Error inserting/updating row for Kafka record {}: {}\n   statement: {}}",
                            record,
                            e.getMessage(),
                            cql);
                } else {
                    log.warn("Error decoding/mapping Kafka record {}: {}", record, e.getMessage());
                }

                CompletableFuture<?> remove = currentRecordStatus.remove(record);

                if (ignoreErrors == CassandraSinkConfig.IgnoreErrorsPolicy.NONE
                        || (ignoreErrors == CassandraSinkConfig.IgnoreErrorsPolicy.DRIVER
                                && !driverFailure)) {
                    remove.completeExceptionally(e);
                } else {
                    remove.complete(null);
                }

                failCounter.run();
            }
        }

        private static class LangStreamSinkRecordAdapter implements AbstractSinkRecord {

            @Getter private final Record record;

            public LangStreamSinkRecordAdapter(Record record) {
                this.record = record;
            }

            @Override
            public Iterable<AbstractSinkRecordHeader> headers() {
                return record.headers().stream()
                        .map(
                                h ->
                                        new AbstractSinkRecordHeader() {

                                            @Override
                                            public AbstractSchema schema() {
                                                return new SimpleStringSchema();
                                            }

                                            @Override
                                            public String key() {
                                                return h.key();
                                            }

                                            @Override
                                            public byte[] value() {
                                                Object v = h.value();
                                                if (v == null) {
                                                    return null;
                                                } else if (v instanceof byte[] b) {
                                                    return b;
                                                } else if (v instanceof String s) {
                                                    return s.getBytes(StandardCharsets.UTF_8);
                                                } else {
                                                    return v.toString()
                                                            .getBytes(StandardCharsets.UTF_8);
                                                }
                                            }
                                        })
                        .collect(Collectors.toList());
            }

            @Override
            public Object key() {
                return record.key();
            }

            @Override
            public Object value() {
                return record.value();
            }

            @Override
            public Long timestamp() {
                return record.timestamp();
            }

            @Override
            public String topic() {
                return DUMMY_TOPIC;
            }

            private static class SimpleStringSchema implements AbstractSchema {
                @Override
                public AbstractSchema valueSchema() {
                    return new SimpleStringSchema();
                }

                @Override
                public AbstractSchema keySchema() {
                    return new SimpleStringSchema();
                }

                @Override
                public Type type() {
                    return Type.STRING;
                }

                @Override
                public List<? extends AbstractField> fields() {
                    return List.of();
                }

                @Override
                public AbstractField field(String name) {
                    return null;
                }
            }
        }
    }
}
