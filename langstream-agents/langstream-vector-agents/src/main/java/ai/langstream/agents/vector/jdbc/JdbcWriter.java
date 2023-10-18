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

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.ai.agents.datasource.impl.JdbcDataSourceProvider;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "jdbc".equals(dataSourceConfig.get("service"));
    }

    @Override
    public JdbcVectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new JdbcVectorDatabaseWriter(datasourceConfig);
    }

    public static class JdbcVectorDatabaseWriter implements VectorDatabaseWriter, AutoCloseable {

        private Connection connection;

        private String tableName;
        private final LinkedHashMap<String, JstlEvaluator> primaryKey = new LinkedHashMap<>();
        private final LinkedHashMap<String, JstlEvaluator> columns = new LinkedHashMap<>();

        private PreparedStatement insert;
        private PreparedStatement update;
        private PreparedStatement delete;
        private Map<String, Object> datasourceConfig;

        public JdbcVectorDatabaseWriter(Map<String, Object> datasourceConfig) {
            this.datasourceConfig = datasourceConfig;
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) throws Exception {

            this.connection = JdbcDataSourceProvider.buildConnection(datasourceConfig);
            this.tableName = ConfigurationUtils.getString("table-name", null, agentConfiguration);

            List<Map<String, Object>> fields =
                    (List<Map<String, Object>>)
                            agentConfiguration.getOrDefault("fields", List.of());
            fields.forEach(
                    field -> {
                        boolean primaryKey =
                                ConfigurationUtils.getBoolean("primary-key", false, field);
                        if (primaryKey) {
                            this.primaryKey.put(
                                    field.get("name").toString(),
                                    buildEvaluator(field, "expression", Object.class));
                        } else {
                            this.columns.put(
                                    field.get("name").toString(),
                                    buildEvaluator(field, "expression", Object.class));
                        }
                    });

            int numParameters = columns.size() + primaryKey.size();
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < numParameters; i++) {
                if (i > 0) {
                    values.append(",");
                }
                values.append("?");
            }
            String insertQuery =
                    "INSERT INTO "
                            + tableName
                            + " ("
                            + String.join(", ", primaryKey.keySet())
                            + ","
                            + String.join(", ", columns.keySet())
                            + ") VALUES ("
                            + values
                            + ")";
            log.info("insertQuery {}", insertQuery);
            insert = connection.prepareStatement(insertQuery);

            String updateQuery =
                    "UPDATE "
                            + tableName
                            + " SET "
                            + String.join("=?, ", columns.keySet())
                            + " = ? WHERE "
                            + String.join("=? AND ", primaryKey.keySet())
                            + "=?";
            update = connection.prepareStatement(updateQuery);
            log.info("updateQuery {}", updateQuery);

            String deleteQuery =
                    "DELETE FROM "
                            + tableName
                            + " WHERE "
                            + String.join("=? AND ", primaryKey.keySet())
                            + "=?";
            delete = connection.prepareStatement(deleteQuery);
            log.info("deleteQuery {}", deleteQuery);
        }

        @Override
        public synchronized CompletableFuture<?> upsert(
                Record record, Map<String, Object> context) {
            CompletableFuture<?> handle = new CompletableFuture<>();
            try {
                MutableRecord mutableRecord = recordToMutableRecord(record, true);

                List<Object> primaryKeyValues = prepareValueList(mutableRecord, primaryKey);
                List<Object> otherValues = prepareValueList(mutableRecord, columns);
                if (record.value() != null) {
                    int i = 1;
                    for (Object value : otherValues) {
                        update.setObject(i++, value);
                    }
                    for (Object value : primaryKeyValues) {
                        update.setObject(i++, value);
                    }
                    int count = update.executeUpdate();
                    if (count == 0) {
                        i = 1;
                        for (Object value : primaryKeyValues) {
                            insert.setObject(i++, value);
                        }
                        for (Object value : otherValues) {
                            insert.setObject(i++, value);
                        }
                        insert.executeUpdate();
                    }
                } else {
                    int i = 1;
                    for (Object value : primaryKeyValues) {
                        delete.setObject(i++, value);
                    }
                    delete.executeUpdate();
                }
                handle.complete(null);
            } catch (Exception e) {
                handle.completeExceptionally(e);
            }
            return handle;
        }

        private List<Object> prepareValueList(
                MutableRecord mutableRecord, Map<String, JstlEvaluator> primaryKey) {
            List<Object> result = new ArrayList<>();
            primaryKey.forEach(
                    (name, evaluator) -> {
                        Object value = evaluator.evaluate(mutableRecord);
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "setting value {} ({}) for field {}",
                                    value,
                                    value.getClass(),
                                    name);
                        }
                        result.add(value);
                    });
            return result;
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
