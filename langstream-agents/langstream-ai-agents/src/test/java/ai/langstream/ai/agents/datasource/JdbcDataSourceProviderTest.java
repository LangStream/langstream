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
package ai.langstream.ai.agents.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.ai.agents.datasource.impl.JdbcDataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class JdbcDataSourceProviderTest {

    @Test
    public void testQuery() throws Exception {
        JdbcDataSourceProvider jdbcDataSourceProvider = new JdbcDataSourceProvider();
        QueryStepDataSource implementation =
                jdbcDataSourceProvider.createDataSourceImplementation(
                        Map.of(
                                "url",
                                "jdbc:h2:mem:test",
                                "user",
                                "sa",
                                "password",
                                "sa",
                                "driverClass",
                                "org.h2.Driver"));
        implementation.initialize(null);

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test", "sa", "sa");
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), price INT)");
            statement.execute("INSERT INTO products (id, name, price) VALUES (1, 'product1', 100)");
            statement.execute("INSERT INTO products (id, name, price) VALUES (2, 'product2', 102)");
        }

        List<Map<String, Object>> results =
                implementation.fetchData(
                        "SELECT name, price from PRODUCTS where id > ?", List.of(1));
        assertEquals(results.size(), 1);
        log.info("Results: {}", results);
        // H2 returns column names in upper case
        assertEquals("product2", results.get(0).get("NAME"));
        assertEquals(102, results.get(0).get("PRICE"));

        implementation.close();
    }
}
