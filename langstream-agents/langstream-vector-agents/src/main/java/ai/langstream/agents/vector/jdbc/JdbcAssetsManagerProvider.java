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
package ai.langstream.agents.vector.jdbc;

import ai.langstream.ai.agents.datasource.impl.JdbcDataSourceProvider;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "jdbc-table".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {
        switch (assetType) {
            case "jdbc-table":
                return new JdbcTableAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class JdbcTableAssetManager implements AssetManager {

        Connection connection;
        AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            Map<String, Object> datasourceConfig =
                    ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
            final Map<String, Object> datasourceConfiguration =
                    ConfigurationUtils.getMap("configuration", Map.of(), datasourceConfig);
            this.assetDefinition = assetDefinition;
            connection = JdbcDataSourceProvider.buildConnection(datasourceConfiguration);
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public boolean assetExists() throws Exception {
            String tableName = getTableName();
            log.info("Checking is table {} exists", tableName);
            DatabaseMetaData metaData = connection.getMetaData();

            // some databases are case sensitive, so we need to check for all cases
            boolean found = findTableInMetadata(metaData, tableName);
            if (!found) {
                found = findTableInMetadata(metaData, tableName.toUpperCase());
            }
            if (!found) {
                found = findTableInMetadata(metaData, tableName.toLowerCase());
            }
            if (found) {
                log.info("Table {} exists", tableName);
            } else {
                log.info("Table {} does not exist", tableName);
            }
            return found;
        }

        private static boolean findTableInMetadata(DatabaseMetaData metaData, String tableName)
                throws SQLException {
            boolean found = false;
            try (ResultSet tables = metaData.getTables(null, null, tableName, null); ) {
                while (tables.next()) {
                    String exitingTableName = tables.getString("TABLE_NAME");
                    if (tableName.equalsIgnoreCase(exitingTableName)) {
                        found = true;
                        break;
                    }
                }
            }
            return found;
        }

        private String getTableName() {
            String tableName =
                    ConfigurationUtils.getString("table-name", null, assetDefinition.getConfig());
            return tableName;
        }

        @Override
        public void deployAsset() throws Exception {
            List<String> statements =
                    ConfigurationUtils.getList("create-statements", assetDefinition.getConfig());
            execStatements(statements);
        }

        private void execStatements(List<String> statements) throws Exception {
            for (String statement : statements) {
                log.info("Executing: {}", statement);
                Statement sqlStatement = connection.createStatement();
                sqlStatement.execute(statement);
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            boolean exists = assetExists();
            if (!exists) {
                log.info(
                        "Table {} does not exist, skipping delete asset statements",
                        getTableName());
                return false;
            }
            List<String> statements =
                    ConfigurationUtils.getList("delete-statements", assetDefinition.getConfig());
            execStatements(statements);
            return true;
        }
    }
}
