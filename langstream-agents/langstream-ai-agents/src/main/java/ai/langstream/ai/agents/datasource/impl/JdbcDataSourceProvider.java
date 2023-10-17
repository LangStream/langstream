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
package ai.langstream.ai.agents.datasource.impl;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "jdbc".equals(dataSourceConfig.get("service"));
    }

    @Override
    public JdbcDataSourceImpl createDataSourceImplementation(Map<String, Object> dataSourceConfig) {
        return new JdbcDataSourceImpl(dataSourceConfig);
    }

    public static class JdbcDataSourceImpl implements QueryStepDataSource {

        Connection connection;
        Map<String, Object> dataSourceConfig;

        public JdbcDataSourceImpl(Map<String, Object> dataSourceConfig) {
            this.dataSourceConfig = dataSourceConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) throws Exception {
            connection = buildConnection(dataSourceConfig);
            connection.setAutoCommit(true);
        }

        @Override
        @SneakyThrows
        public Map<String, Object> executeStatement(
                String query, List<String> generatedKeys, List<Object> params) {
            PreparedStatement ps;
            if (generatedKeys != null && !generatedKeys.isEmpty()) {
                ps = connection.prepareStatement(query, generatedKeys.toArray(new String[0]));
            } else {
                ps = connection.prepareStatement(query);
            }
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            long resultCount = ps.executeLargeUpdate();
            Map<String, Object> generatedKeysValues = null;
            if (generatedKeys != null && !generatedKeys.isEmpty()) {
                try (ResultSet resultSet = ps.getGeneratedKeys(); ) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int numColumns = metaData.getColumnCount();
                    generatedKeysValues = new HashMap<>();
                    while (resultSet.next()) {

                        for (int i = 1; i <= numColumns; i++) {
                            Object value = resultSet.getObject(i);
                            generatedKeysValues.put(metaData.getColumnName(i), value);
                        }
                    }
                }
            }
            if (generatedKeysValues != null) {
                return Map.of("count", resultCount, "generatedKeys", generatedKeysValues);
            } else {
                return Map.of("count", resultCount);
            }
        }

        @Override
        @SneakyThrows
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            PreparedStatement ps = connection.prepareStatement(query);
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet resultSet = ps.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int numColumns = metaData.getColumnCount();
                List<Map<String, Object>> results = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, Object> result = new HashMap<>();
                    for (int i = 1; i <= numColumns; i++) {
                        Object value = resultSet.getObject(i);
                        result.put(metaData.getColumnName(i), value);
                    }
                    results.add(result);
                }
                return results;
            }
        }

        @Override
        public void close() {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    log.error("Error closing connection", e);
                }
            }
        }
    }

    public static Connection buildConnection(Map<String, Object> dataSourceConfig)
            throws InstantiationException,
                    IllegalAccessException,
                    InvocationTargetException,
                    NoSuchMethodException,
                    ClassNotFoundException,
                    SQLException {
        Properties properties = new Properties();
        properties.putAll(dataSourceConfig);
        String driverClass = properties.getProperty("driverClass", "");
        log.info("Connecting to {}, config {}", properties.getProperty("url"), properties);
        if (!driverClass.isEmpty()) {
            ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            log.info("Loading JDBC Driver {} from classloader {}", driverClass, currentClassLoader);
            Driver driver =
                    (Driver)
                            Class.forName(driverClass, true, currentClassLoader)
                                    .getConstructor()
                                    .newInstance();
            // https://www.kfu.com/~nsayer/Java/dyn-jdbc.html
            DriverManager.registerDriver(new DriverShim(driver));
        }
        return DriverManager.getConnection((String) properties.get("url"), properties);
    }

    static class DriverShim implements Driver {
        private final Driver driver;

        DriverShim(Driver d) {
            this.driver = d;
        }

        public boolean acceptsURL(String u) throws SQLException {
            return this.driver.acceptsURL(u);
        }

        public Connection connect(String u, Properties p) throws SQLException {
            return this.driver.connect(u, p);
        }

        public int getMajorVersion() {
            return this.driver.getMajorVersion();
        }

        public int getMinorVersion() {
            return this.driver.getMinorVersion();
        }

        public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
            return this.driver.getPropertyInfo(u, p);
        }

        public boolean jdbcCompliant() {
            return this.driver.jdbcCompliant();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return this.driver.getParentLogger();
        }
    }
}
