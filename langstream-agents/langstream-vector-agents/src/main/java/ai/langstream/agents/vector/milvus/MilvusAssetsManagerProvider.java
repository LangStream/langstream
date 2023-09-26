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

import static ai.langstream.agents.vector.InterpolationUtils.buildObjectFromJson;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "milvus-collection".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "milvus-collection":
                return new MilvusCollectionAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class MilvusCollectionAssetManager implements AssetManager {

        MilvusDataSource.MilvusQueryStepDataSource datasource;
        AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.datasource = buildDataSource(assetDefinition);
            this.assetDefinition = assetDefinition;
        }

        @Override
        public boolean assetExists() throws Exception {
            String collectionName = getCollectionName();
            String databaseName = getDatabaseName();
            log.info(
                    "Checking is collection {} exists in database {}",
                    collectionName,
                    databaseName);

            MilvusServiceClient milvusClient = datasource.getMilvusClient();
            R<Boolean> hasCollection =
                    milvusClient.hasCollection(
                            HasCollectionParam.newBuilder()
                                    .withCollectionName(collectionName)
                                    .withDatabaseName(databaseName)
                                    .build());

            MilvusModel.handleException(hasCollection);

            if (hasCollection.getData() != null && hasCollection.getData()) {
                log.info("Table {} exists", collectionName);
                R<DescribeCollectionResponse> describeCollectionResponseR =
                        milvusClient.describeCollection(
                                DescribeCollectionParam.newBuilder()
                                        .withCollectionName(collectionName)
                                        .withDatabaseName(databaseName)
                                        .build());
                log.info("Describe table result: {}", describeCollectionResponseR.getData());
                return true;
            } else {
                log.info("Table {} does not exist", collectionName);
                return false;
            }
        }

        private String getCollectionName() {
            return ConfigurationUtils.getString(
                    "collection-name", null, assetDefinition.getConfig());
        }

        private String getDatabaseName() {
            return ConfigurationUtils.getString("database-name", null, assetDefinition.getConfig());
        }

        @Override
        public void deployAsset() throws Exception {
            List<String> statements =
                    ConfigurationUtils.getList("create-statements", assetDefinition.getConfig());
            execStatements(statements);

            MilvusServiceClient milvusClient = datasource.getMilvusClient();
            DescribeCollectionParam.Builder builder =
                    DescribeCollectionParam.newBuilder().withCollectionName(getCollectionName());
            String databaseName = getDatabaseName();
            if (databaseName != null && !databaseName.isEmpty()) {
                builder.withDatabaseName(databaseName);
            }
            R<DescribeCollectionResponse> describeCollectionResponse =
                    milvusClient.describeCollection(builder.build());
            MilvusModel.handleException(describeCollectionResponse);

            log.info(
                    "Describe collection {} in database {}",
                    getCollectionName(),
                    getDatabaseName());
            log.info("Result: {}", describeCollectionResponse.getData());
        }

        private void execStatements(List<String> statements) throws Exception {
            MilvusServiceClient milvusClient = datasource.getMilvusClient();
            for (String statement : statements) {
                log.info("Executing: {}", statement);

                Map<String, Object> statementMap =
                        MilvusModel.getMapper()
                                .readValue(statement, new TypeReference<Map<String, Object>>() {});
                String command = ConfigurationUtils.getString("command", "", statementMap);

                if (command.isEmpty()) {
                    throw new IllegalArgumentException("Command is empty");
                }

                switch (command) {
                    case "create-collection":
                        {
                            CreateCollectionParam parsedQuery =
                                    buildObjectFromJson(
                                                    statement,
                                                    CreateCollectionParam.Builder.class,
                                                    List.of(),
                                                    MilvusModel.getMapper())
                                            .build();
                            log.info("Command: {}", parsedQuery);
                            R<RpcStatus> resultCreate = milvusClient.createCollection(parsedQuery);

                            MilvusModel.handleException(resultCreate);
                            break;
                        }
                    case "create-index":
                        {
                            CreateIndexParam parsedQuery =
                                    buildObjectFromJson(
                                                    statement,
                                                    CreateIndexParam.Builder.class,
                                                    List.of(),
                                                    MilvusModel.getMapper())
                                            .build();
                            log.info("Command: {}", parsedQuery);

                            R<RpcStatus> indexResult = milvusClient.createIndex(parsedQuery);
                            MilvusModel.handleException(indexResult);
                            break;
                        }
                    case "load-collection":
                        {
                            R<RpcStatus> result =
                                    milvusClient.loadCollection(
                                            LoadCollectionParam.newBuilder()
                                                    .withCollectionName(getCollectionName())
                                                    .withDatabaseName(getDatabaseName())
                                                    .withSyncLoad(true)
                                                    .withSyncLoadWaitingInterval(2000L)
                                                    .build());

                            MilvusModel.handleException(result);
                            break;
                        }
                    case "create-simple-collection":
                        {
                            CreateSimpleCollectionParam parsedQuery =
                                    buildObjectFromJson(
                                                    statement,
                                                    CreateSimpleCollectionParam.Builder.class,
                                                    List.of(),
                                                    MilvusModel.getMapper())
                                            .build();
                            log.info("Command: {}", parsedQuery);
                            R<RpcStatus> createResult = milvusClient.createCollection(parsedQuery);
                            MilvusModel.handleException(createResult);
                            break;
                        }
                    default:
                        throw new IllegalStateException("Unexpected command: " + command);
                }
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            if (!assetExists()) {
                return false;
            }
            MilvusServiceClient milvusClient = datasource.getMilvusClient();
            String collectionName = getCollectionName();
            String databaseName = getDatabaseName();
            milvusClient.dropCollection(
                    DropCollectionParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withDatabaseName(databaseName)
                            .build());

            return true;
        }

        @Override
        public void close() throws Exception {
            if (datasource != null) {
                datasource.close();
            }
        }
    }

    private static MilvusDataSource.MilvusQueryStepDataSource buildDataSource(
            AssetDefinition assetDefinition) {
        MilvusDataSource dataSource = new MilvusDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        MilvusDataSource.MilvusQueryStepDataSource result =
                dataSource.createDataSourceImplementation(configuration);
        result.initialize(null);
        return result;
    }
}
