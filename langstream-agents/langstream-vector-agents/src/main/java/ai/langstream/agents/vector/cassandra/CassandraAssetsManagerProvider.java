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

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.streaming.ai.datasource.CassandraDataSource;
import com.dtsx.astra.sdk.db.DbOpsClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "cassandra-table".equals(assetType)
                || "cassandra-keyspace".equals(assetType)
                || "astra-keyspace".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "cassandra-table":
                return new CassandraTableAssetManager();
            case "cassandra-keyspace":
                return new CassandraKeyspaceAssetManager();
            case "astra-keyspace":
                return new AstraDBKeyspaceAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class CassandraTableAssetManager extends BaseCassandraAssetManager {

        @Override
        public boolean assetExists() throws Exception {
            String tableName = getTableName();
            String keySpace = getKeySpace();
            log.info("Checking is table {} exists in keyspace {}", tableName, keySpace);

            CqlSession session = datasource.getSession();
            log.info("Getting keyspace {} metadata", keySpace);
            Optional<KeyspaceMetadata> keyspace = session.getMetadata().getKeyspace(keySpace);
            if (keyspace.isEmpty()) {
                throw new IllegalStateException(
                        "The keyspace "
                                + keySpace
                                + " does not exist, "
                                + "you could use a cassandra-keyspace asset to create it.");
            }
            log.info("Getting table {} metadata", tableName);
            KeyspaceMetadata keyspaceMetadata = keyspace.get();
            Optional<TableMetadata> table = keyspaceMetadata.getTable(tableName);

            if (table.isPresent()) {
                log.info("Table {} exists", tableName);
                String describe = table.get().describe(true);
                log.info("Describe table result: {}", describe);
            } else {
                log.info("Table {} does not exist", tableName);
            }
            return table.isPresent();
        }

        private String getTableName() {
            String tableName =
                    ConfigurationUtils.getString("table-name", null, assetDefinition.getConfig());
            return tableName;
        }

        @Override
        public void deployAsset() throws Exception {
            String keySpace = getKeySpace();
            List<String> statements =
                    ConfigurationUtils.getList("create-statements", assetDefinition.getConfig());
            execStatements(keySpace, statements);
        }

        private void execStatements(String keySpace, List<String> statements) {
            for (String statement : statements) {
                log.info("Executing: {}", statement);
                SimpleStatement simpleStatement = SimpleStatement.newInstance(statement);
                simpleStatement.setKeyspace(keySpace);
                try {
                    datasource.getSession().execute(simpleStatement);
                } catch (AlreadyExistsException e) {
                    log.info(
                            "Table already exists, maybe it was created by another agent ({})",
                            e.toString());
                }
            }
        }

        private String getKeySpace() {
            String keySpace =
                    ConfigurationUtils.getString("keyspace", null, assetDefinition.getConfig());
            return keySpace;
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            final String keySpace = getKeySpace();
            List<String> statements =
                    ConfigurationUtils.getList("delete-statements", assetDefinition.getConfig());
            execStatements(keySpace, statements);
            return true;
        }
    }

    private static class CassandraKeyspaceAssetManager extends BaseCassandraAssetManager {

        @Override
        public boolean assetExists() throws Exception {
            String keySpace = getKeyspace();
            log.info("Checking if keyspace {} exists", keySpace);

            CqlSession session = datasource.getSession();
            Optional<KeyspaceMetadata> keyspace = session.getMetadata().getKeyspace(keySpace);
            keyspace.ifPresent(
                    keyspaceMetadata ->
                            log.info(
                                    "Describe keyspace result: {}",
                                    keyspaceMetadata.describe(true)));
            log.info("Result: {}", keyspace);
            return keyspace.isPresent();
        }

        private String getKeyspace() {
            String keySpace =
                    ConfigurationUtils.getString("keyspace", null, assetDefinition.getConfig());
            return keySpace;
        }

        @Override
        public void deployAsset() throws Exception {
            List<String> statements =
                    ConfigurationUtils.getList("create-statements", assetDefinition.getConfig());
            execStatements(statements);
        }

        private void execStatements(List<String> statements) {
            for (String statement : statements) {
                log.info("Executing: {}", statement);
                try {
                    datasource.executeStatement(statement, List.of());
                } catch (AlreadyExistsException e) {
                    log.info(
                            "Keyspace already exists, maybe it was created by another agent ({})",
                            e.toString());
                }
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            List<String> statements =
                    ConfigurationUtils.getList("delete-statements", assetDefinition.getConfig());
            execStatements(statements);
            return true;
        }
    }

    private abstract static class BaseCassandraAssetManager implements AssetManager {

        CassandraDataSource datasource;
        AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) {
            this.datasource = buildDataSource(assetDefinition);
            this.assetDefinition = assetDefinition;
        }

        @Override
        public void close() throws Exception {
            if (datasource != null) {
                datasource.close();
            }
        }
    }

    private static class AstraDBKeyspaceAssetManager extends BaseCassandraAssetManager {

        @Override
        public boolean assetExists() throws Exception {
            String keySpace = getKeyspace();
            log.info("Checking if keyspace {} exists", keySpace);
            DbOpsClient astraDbClient = datasource.buildAstraClient();
            boolean exist = astraDbClient.keyspaces().exist(keySpace);
            log.info("Result: {}", exist);
            return exist;
        }

        @Override
        public void deployAsset() throws Exception {
            String keySpace = getKeyspace();
            DbOpsClient astraDbClient = datasource.buildAstraClient();
            try {
                astraDbClient.keyspaces().create(keySpace);
            } catch (com.dtsx.astra.sdk.db.exception.KeyspaceAlreadyExistException e) {
                log.info(
                        "Keyspace already exists, maybe it was created by another agent ({})",
                        e.toString());
            } catch (com.dtsx.astra.sdk.exception.AuthenticationException e) {
                String message = e.toString();
                if (message.contains("HTTP_CONFLICT")) {
                    log.info(
                            "Keyspace already exists, maybe it was created by another agent ({})",
                            e.toString());
                } else {
                    throw new IllegalStateException("Error creating keyspace", e);
                }
            }
        }

        private String getKeyspace() {
            String keySpace =
                    ConfigurationUtils.getString("keyspace", null, assetDefinition.getConfig());
            return keySpace;
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            String keySpace = getKeyspace();

            log.info("Deleting keyspace {}", keySpace);
            DbOpsClient astraDbClient = datasource.buildAstraClient();
            try {
                astraDbClient.keyspaces().delete(keySpace);
                return true;
            } catch (com.dtsx.astra.sdk.db.exception.KeyspaceNotFoundException e) {
                log.info(
                        "Keyspace does not exist, maybe it was deleted by another agent ({})",
                        e.toString());
                return false;
            }
        }
    }

    private static CassandraDataSource buildDataSource(AssetDefinition assetDefinition) {
        CassandraDataSource dataSource = new CassandraDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        dataSource.initialize(configuration);
        return dataSource;
    }
}
