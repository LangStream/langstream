package com.datastax.oss.sga.ai.agents.datasource;

import com.datastax.oss.sga.ai.agents.datasource.impl.JdbcDataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class JdbcDataSourceProviderTest {

    @Test
    public void testQuery() throws Exception {
        JdbcDataSourceProvider jdbcDataSourceProvider = new JdbcDataSourceProvider();
        QueryStepDataSource implementation = jdbcDataSourceProvider.createImplementation(Map.of("url", "jdbc:h2:mem:test",
                "user", "sa", "password", "sa", "driverClass", "org.h2.Driver"));

        try (Connection conn = DriverManager.
                getConnection("jdbc:h2:mem:test", "sa", "sa");
                Statement statement = conn.createStatement();) {
            statement.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), price INT)");
            statement.execute("INSERT INTO products (id, name, price) VALUES (1, 'product1', 100)");
            statement.execute("INSERT INTO products (id, name, price) VALUES (2, 'product2', 102)");
        }

        List<Map<String, String>> results = implementation.fetchData("SELECT name, price from PRODUCTS where id > ?", List.of(1));
        assertEquals(results.size(), 1);
        log.info("Results: {}", results);
        // H2 returns column names in upper case
        assertEquals("product2", results.get(0).get("NAME"));
        assertEquals("102", results.get(0).get("PRICE") );

        implementation.close();
    }

}
