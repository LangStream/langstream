package ai.langstream.agents.vector.couchbase;

import ai.langstream.agents.vector.InterpolationUtils;
import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryResult;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "couchbase".equals(dataSourceConfig.get("service"));
    }

    @Getter
    public static class CouchbaseConfig {
        private String connectionString;
        private String username;
        private String password;
        private String bucketName;
    }

    @Override
    public QueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {
        CouchbaseConfig config = MAPPER.convertValue(dataSourceConfig, CouchbaseConfig.class);
        return new CouchbaseQueryStepDataSource(config);
    }

    public static class CouchbaseQueryStepDataSource implements QueryStepDataSource {

        @Getter private final CouchbaseConfig clientConfig;
        private Cluster cluster;
        private Collection collection;

        public CouchbaseQueryStepDataSource(CouchbaseConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            cluster =
                    Cluster.connect(
                            clientConfig.connectionString,
                            clientConfig.username,
                            clientConfig.password);
            collection = cluster.bucket(clientConfig.bucketName).defaultCollection();
            log.info("Connected to Couchbase Bucket: {}", clientConfig.bucketName);
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                String finalQuery = InterpolationUtils.interpolate(query, params);
                QueryResult result = cluster.query(finalQuery);
                return result.rowsAsObject().stream().map(row -> row.toMap()).toList();
            } catch (Exception e) {
                log.error("Error executing query: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (cluster != null) {
                cluster.disconnect();
            }
        }
    }
}
