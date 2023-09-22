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
package ai.langstream.agents.vector.milvus;

import ai.langstream.ai.agents.GenAIToolKitAgent;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.jstl.JstlEvaluator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.MutationResult;
import io.milvus.param.R;
import io.milvus.param.dml.UpsertParam;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusWriter implements VectorDatabaseWriterProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "milvus".equals(dataSourceConfig.get("service"));
    }

    @Override
    public MilvusVectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new MilvusVectorDatabaseWriter(datasourceConfig);
    }

    public static class MilvusVectorDatabaseWriter implements VectorDatabaseWriter, AutoCloseable {

        private final MilvusDataSource.MilvusQueryStepDataSource dataSource;
        private String collectionName;
        private String databaseName;
        private Map<String, JstlEvaluator> fields = new HashMap<>();

        public MilvusVectorDatabaseWriter(Map<String, Object> datasourceConfig) {
            MilvusDataSource dataSourceProvider = new MilvusDataSource();
            dataSource = dataSourceProvider.createDataSourceImplementation(datasourceConfig);
        }

        @Override
        public void close() throws Exception {
            dataSource.close();
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) {
            this.collectionName =
                    ConfigurationUtils.getString("collection-name", "", agentConfiguration);
            this.databaseName =
                    ConfigurationUtils.getString("database-name", "", agentConfiguration);
            List<Map<String, Object>> fields =
                    (List<Map<String, Object>>)
                            agentConfiguration.getOrDefault("fields", List.of());
            fields.forEach(
                    field -> {
                        this.fields.put(
                                field.get("name").toString(),
                                buildEvaluator(field, "expression", Object.class));
                    });
            dataSource.initialize(null);
        }

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {
            CompletableFuture<?> handle = new CompletableFuture<>();
            try {
                TransformContext transformContext =
                        GenAIToolKitAgent.recordToTransformContext(record, true);

                MilvusServiceClient milvusClient = dataSource.getMilvusClient();

                UpsertParam.Builder builder = UpsertParam.newBuilder();
                builder.withCollectionName(collectionName);

                if (databaseName != null && !databaseName.isEmpty()) {
                    // this doesn't work at the moment, see
                    // https://github.com/milvus-io/milvus-sdk-java/pull/644
                    builder.withDatabaseName(databaseName);
                }

                JSONObject row = new JSONObject();
                fields.forEach(
                        (name, evaluator) -> {
                            Object value = evaluator.evaluate(transformContext);
                            row.put(name, value);
                        });
                builder.withRows(List.of(row));
                UpsertParam upsert = builder.build();

                R<MutationResult> upsertResponse = milvusClient.upsert(upsert);
                log.info("Result {}", upsertResponse);
                if (upsertResponse.getException() != null) {
                    handle.completeExceptionally(upsertResponse.getException());
                } else {
                    handle.complete(null);
                }
            } catch (Exception e) {
                handle.completeExceptionally(e);
            }
            return handle;
        }
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
