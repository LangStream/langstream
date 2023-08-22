/**
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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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

@Slf4j
public class JdbcDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "jdbc".equals(dataSourceConfig.get("service"));
    }

    @Override
    @SneakyThrows
    public QueryStepDataSource createDataSourceImplementation(Map<String, Object> dataSourceConfig){
        return new DataSourceImpl(dataSourceConfig);
    }

    private static class DataSourceImpl implements QueryStepDataSource {

        private final Properties properties;

        Connection connection;

        public DataSourceImpl(Map<String, Object> dataSourceConfig) throws Exception {
            properties = new Properties();
            properties.putAll(dataSourceConfig);
            String driverClass = properties.getProperty("driverClass", "");
            log.info("Connecting to {}, config {}", properties.getProperty("url"), properties);
            if (!driverClass.isEmpty()) {
                log.info("Loading JDBC Driver {}", driverClass);
                Driver driver = (Driver) Class.forName(driverClass, true, Thread.currentThread().getContextClassLoader())
                        .getConstructor().newInstance();
                // https://www.kfu.com/~nsayer/Java/dyn-jdbc.html
                DriverManager.registerDriver(new DriverShim(driver));
            }
            connection = DriverManager.getConnection((String) properties.get("url"), properties);
            connection.setAutoCommit(true);
        }

        @Override
        @SneakyThrows
        public List<Map<String, String>> fetchData(String query, List<Object> params) {
            PreparedStatement ps = connection.prepareStatement(query);
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet resultSet = ps.executeQuery();) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int numColumns = metaData.getColumnCount();
                List<Map<String, String>> results = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 1; i <= numColumns; i++) {
                        Object value = resultSet.getObject(i);
                        result.put(metaData.getColumnName(i), value != null ? value.toString() : null);
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

    static class DriverShim implements Driver {
        private Driver driver;
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
