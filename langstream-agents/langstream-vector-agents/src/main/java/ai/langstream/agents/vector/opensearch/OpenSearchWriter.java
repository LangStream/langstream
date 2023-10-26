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
package ai.langstream.agents.vector.opensearch;

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.api.util.OrderedAsyncBatchExecutor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch._types.WaitForActiveShardOptions;
import org.opensearch.client.opensearch._types.WaitForActiveShards;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;

@Slf4j
public class OpenSearchWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "opensearch".equals(dataSourceConfig.get("service"));
    }

    @Override
    public OpenSearchVectorDatabaseWriter createImplementation(
            Map<String, Object> datasourceConfig) {
        return new OpenSearchVectorDatabaseWriter(datasourceConfig);
    }

    public static class OpenSearchVectorDatabaseWriter
            implements VectorDatabaseWriter, AutoCloseable {

        protected static final ObjectMapper OBJECT_MAPPER =
                new ObjectMapper()
                        .configure(SerializationFeature.INDENT_OUTPUT, false)
                        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

        @Getter private final OpenSearchDataSource.OpenSearchQueryStepDataSource dataSource;

        private OrderedAsyncBatchExecutor<OpenSearchRecord> batchExecutor;
        private ScheduledExecutorService executorService;

        private String indexName;
        private JstlEvaluator id;
        private Map<String, JstlEvaluator> fields = new HashMap<>();
        private BulkParameters bulkParameters;

        public OpenSearchVectorDatabaseWriter(Map<String, Object> datasourceConfig) {
            OpenSearchDataSource dataSourceProvider = new OpenSearchDataSource();
            dataSource = dataSourceProvider.createDataSourceImplementation(datasourceConfig);
        }

        @Override
        public void close() throws Exception {
            dataSource.close();
        }

        @Data
        public static class BulkParameters {
            String pipeline;

            String refresh;

            @JsonProperty("require_alias")
            Boolean requireAlias;

            String routing;
            String timeout;

            @JsonProperty("wait_for_active_shards")
            String waitForActiveShards;
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) throws Exception {
            dataSource.initialize(null);
            indexName = dataSource.getClientConfig().getIndexName();
            List<Map<String, Object>> fields =
                    (List<Map<String, Object>>)
                            agentConfiguration.getOrDefault("fields", List.of());
            fields.forEach(
                    field -> {
                        this.fields.put(
                                field.get("name").toString(),
                                buildEvaluator(field, "expression", Object.class));
                    });
            id = buildEvaluator(agentConfiguration, "id", String.class);
            bulkParameters =
                    OBJECT_MAPPER.convertValue(
                            ConfigurationUtils.getMap(
                                    "bulk-parameters", Map.of(), agentConfiguration),
                            BulkParameters.class);

            final int flushInterval =
                    ConfigurationUtils.getInt("flush-interval", 1000, agentConfiguration);
            final int batchSize = ConfigurationUtils.getInt("batch-size", 10, agentConfiguration);
            this.executorService =
                    flushInterval > 0 ? Executors.newSingleThreadScheduledExecutor() : null;

            this.batchExecutor =
                    new OrderedAsyncBatchExecutor<>(
                            batchSize,
                            (records, completableFuture) -> {
                                try {

                                    List<BulkOperation> bulkOps = new ArrayList<>();

                                    for (OpenSearchRecord record : records) {
                                        boolean delete = record.document() == null;
                                        final BulkOperation bulkOp;
                                        if (!delete) {
                                            log.info(
                                                    "indexing document {} with id {} on index {}",
                                                    record.document(),
                                                    record.id(),
                                                    indexName);
                                            final IndexOperation<Object> request =
                                                    new IndexOperation.Builder<>()
                                                            .index(indexName)
                                                            .document(record.document())
                                                            .id(record.id())
                                                            .build();
                                            bulkOp =
                                                    new BulkOperation.Builder()
                                                            .index(request)
                                                            .build();
                                        } else {
                                            log.info(
                                                    "deleting document with id {} on index {}",
                                                    record.id(),
                                                    indexName);
                                            final DeleteOperation request =
                                                    new DeleteOperation.Builder()
                                                            .index(indexName)
                                                            .id(record.id())
                                                            .build();
                                            bulkOp =
                                                    new BulkOperation.Builder()
                                                            .delete(request)
                                                            .build();
                                        }
                                        bulkOps.add(bulkOp);
                                    }

                                    final BulkRequest.Builder bulkBuilder =
                                            new BulkRequest.Builder();
                                    bulkBuilder.pipeline(bulkParameters.getPipeline());
                                    bulkBuilder.refresh(getRefreshValue());
                                    bulkBuilder.requireAlias(bulkParameters.getRequireAlias());
                                    bulkBuilder.routing(bulkParameters.getRouting());
                                    if (bulkParameters.getTimeout() != null) {
                                        bulkBuilder.timeout(
                                                new Time.Builder()
                                                        .time(bulkParameters.getTimeout())
                                                        .build());
                                    }
                                    if (bulkParameters.getWaitForActiveShards() != null) {
                                        final WaitForActiveShards value;
                                        if (bulkParameters.getWaitForActiveShards().equals("all")) {
                                            value =
                                                    new WaitForActiveShards.Builder()
                                                            .option(WaitForActiveShardOptions.All)
                                                            .build();
                                        } else {
                                            value =
                                                    new WaitForActiveShards.Builder()
                                                            .count(
                                                                    Integer.parseInt(
                                                                            bulkParameters
                                                                                    .getWaitForActiveShards()))
                                                            .build();
                                        }
                                        bulkBuilder.waitForActiveShards(value);
                                    }

                                    final BulkRequest bulkRequest =
                                            bulkBuilder
                                                    .index(indexName)
                                                    .operations(bulkOps)
                                                    .build();
                                    final BulkResponse response;
                                    try {
                                        response = dataSource.getClient().bulk(bulkRequest);
                                    } catch (IOException e) {
                                        log.error(
                                                "Error indexing documents on index {}: {}",
                                                indexName,
                                                e.getMessage(),
                                                e);
                                        for (OpenSearchRecord record : records) {
                                            record.completableFuture().completeExceptionally(e);
                                        }
                                        completableFuture.completeExceptionally(e);
                                        return;
                                    }
                                    int itemIndex = 0;
                                    boolean failures = false;
                                    for (BulkResponseItem item : response.items()) {
                                        if (item.error() != null) {
                                            String errorString =
                                                    item.error().type()
                                                            + " - "
                                                            + item.error().reason();
                                            ;
                                            log.error(
                                                    "Error indexing document {} on index {}: {}",
                                                    item.id(),
                                                    indexName,
                                                    errorString);
                                            failures = true;
                                            records.get(itemIndex++)
                                                    .completableFuture()
                                                    .completeExceptionally(
                                                            new RuntimeException(
                                                                    "Error indexing document: "
                                                                            + errorString));
                                        } else {
                                            records.get(itemIndex++)
                                                    .completableFuture()
                                                    .complete(null);
                                        }
                                    }
                                    if (!failures) {
                                        log.info(
                                                "Indexed {} documents on index {}",
                                                records.size(),
                                                indexName);
                                        completableFuture.complete(null);
                                    } else {
                                        completableFuture.completeExceptionally(
                                                new RuntimeException("Error indexing documents"));
                                    }
                                } catch (Throwable e) {
                                    log.error(
                                            "Error indexing documents on index {}: {}",
                                            indexName,
                                            e.getMessage(),
                                            e);
                                    for (OpenSearchRecord record : records) {
                                        record.completableFuture().completeExceptionally(e);
                                    }
                                    completableFuture.completeExceptionally(e);
                                }
                            },
                            flushInterval,
                            1,
                            (__) -> 0,
                            executorService);
            batchExecutor.start();
        }

        private Refresh getRefreshValue() {
            if (bulkParameters.getRefresh() == null) {
                return null;
            }
            for (Refresh value : Refresh.values()) {
                if (bulkParameters.getRefresh().equals(value.jsonValue())) {
                    return value;
                }
            }
            throw new IllegalArgumentException(
                    "Invalid refresh value: " + bulkParameters.getRefresh());
        }

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {

            CompletableFuture<?> handle = new CompletableFuture<>();
            try {
                MutableRecord mutableRecord = recordToMutableRecord(record, true);
                Map<String, Object> documentJson;

                if (record.value() != null) {
                    documentJson = new HashMap<>();
                    fields.forEach(
                            (name, evaluator) -> {
                                Object value = evaluator.evaluate(mutableRecord);
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "setting value {} ({}) for field {}",
                                            value,
                                            value.getClass(),
                                            name);
                                }
                                documentJson.put(name, value);
                            });
                } else {
                    documentJson = null;
                }

                final String documentId;
                if (id == null) {
                    documentId = null;
                } else {
                    final Object evaluate = id.evaluate(mutableRecord);
                    documentId = evaluate == null ? null : evaluate.toString();
                }
                if (documentJson == null && documentId == null) {
                    log.info("skipping null document and id, was record: {}", record);
                    return CompletableFuture.completedFuture(null);
                }
                batchExecutor.add(new OpenSearchRecord(documentId, documentJson, handle));
            } catch (Exception e) {
                handle.completeExceptionally(e);
            }
            return handle;
        }

        record OpenSearchRecord(
                String id, Map<String, Object> document, CompletableFuture<?> completableFuture) {}
    }

    private static JstlEvaluator buildEvaluator(
            Map<String, Object> agentConfiguration, String param, Class type) {
        String expression = agentConfiguration.getOrDefault(param, "").toString();
        if (expression == null || expression.isEmpty()) {
            return null;
        }
        return new JstlEvaluator("${" + expression + "}", type);
    }
}
