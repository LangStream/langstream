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
package ai.langstream.agents.vector.astra;

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import io.stargate.sdk.json.CollectionClient;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.exception.ApiException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraVectorDBWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "astra-vector-db".equals(dataSourceConfig.get("service"));
    }

    @Override
    public VectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new AstraCollectionsDatabaseWriter(datasourceConfig);
    }

    private static class AstraCollectionsDatabaseWriter implements VectorDatabaseWriter {

        AstraVectorDBDataSource dataSource;
        private final Map<String, Object> datasourceConfig;
        private String collectionName;
        private CollectionClient collection;

        private final LinkedHashMap<String, JstlEvaluator> fields = new LinkedHashMap<>();

        public AstraCollectionsDatabaseWriter(Map<String, Object> datasourceConfig) {
            this.datasourceConfig = datasourceConfig;
            this.dataSource = new AstraVectorDBDataSource();
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) {
            collectionName =
                    ConfigurationUtils.getString("collection-name", "", agentConfiguration);
            dataSource.initialize(datasourceConfig);
            collection = dataSource.getAstraDB().collection(collectionName);

            List<Map<String, Object>> fields =
                    (List<Map<String, Object>>)
                            agentConfiguration.getOrDefault("fields", List.of());
            fields.forEach(
                    field -> {
                        this.fields.put(
                                field.get("name").toString(),
                                buildEvaluator(field, "expression", Object.class));
                    });
        }

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {
            MutableRecord mutableRecord = recordToMutableRecord(record, true);
            JsonDocument document = new JsonDocument();
            try {
                computeFields(
                        mutableRecord,
                        fields,
                        (name, value) -> {
                            if (value != null) {
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "setting value {} ({}) for field {}",
                                            value,
                                            value.getClass(),
                                            name);
                                }
                                switch (name) {
                                    case "vector":
                                        document.vector(JstlFunctions.toArrayOfFloat(value));
                                        break;
                                    case "id":
                                        document.id(value.toString());
                                        break;
                                    case "data":
                                        document.data(value);
                                        break;
                                    default:
                                        document.put(name, value);
                                        break;
                                }
                            }
                        });
                // ensure that we always have an ID
                if (document.getId() == null) {
                    document.setId(UUID.randomUUID().toString());
                }
                if (record.value() == null) {
                    int count = collection.deleteById(document.getId());
                    if (log.isDebugEnabled()) {
                        if (count > 0) {
                            log.debug("Deleted document with id {}", document.getId());
                        } else {
                            log.debug("No document with id {} to delete", document.getId());
                        }
                    }
                    return CompletableFuture.completedFuture(document.getId());
                } else {

                    try {
                        String id = collection.insertOne(document);
                        if (log.isDebugEnabled()) {
                            log.debug("Inserted document with id {}", id);
                        }
                        return CompletableFuture.completedFuture(id);
                    } catch (ApiException e) {
                        if ("DOCUMENT_ALREADY_EXISTS".equals(e.getErrorCode())) {
                            collection. // Already Exist
                                    findOneAndReplace(
                                    UpdateQuery.builder()
                                            .where("_id")
                                            .isEqualsTo(document.getId())
                                            .replaceBy(document)
                                            .build());
                            return CompletableFuture.completedFuture(document.getId());
                        } else {
                            return CompletableFuture.failedFuture(e);
                        }
                    }
                }

            } catch (Throwable e) {
                log.error("Error while inserting document {}", document, e);
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public void close() {}

        private void computeFields(
                MutableRecord mutableRecord,
                Map<String, JstlEvaluator> fields,
                BiConsumer<String, Object> acceptor) {
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
                        acceptor.accept(name, value);
                    });
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
}
