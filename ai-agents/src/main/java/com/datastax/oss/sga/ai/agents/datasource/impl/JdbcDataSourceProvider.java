package com.datastax.oss.sga.ai.agents.datasource.impl;

import com.datastax.oss.sga.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class JdbcDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "jdbc".equals(dataSourceConfig.get("service"));
    }

    @Override
    public QueryStepDataSource createImplementation(Map<String, Object> dataSourceConfig) {
        return new DataSourceImpl(dataSourceConfig);
    }

    private static class DataSourceImpl implements QueryStepDataSource {

        private final Properties properties;

        Connection connection;

        public DataSourceImpl(Map<String, Object> dataSourceConfig) {
            properties = new Properties();
            properties.putAll(dataSourceConfig);
        }

        @Override
        @SneakyThrows
        public void initialize(DataSourceConfig config) {
            String driverClass = properties.getProperty("driverClass", "");
            if (!driverClass.isEmpty()) {
                Class.forName(driverClass, true, Thread.currentThread().getContextClassLoader());
            }
            log.info("Connecting to {}, config {}", properties.getProperty("url"), properties);
            connection = DriverManager.getConnection((String) properties.get("url"), properties);
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
}
