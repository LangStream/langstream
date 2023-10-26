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
package ai.langstream.agents.vector.solr;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SolrAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "solr-collection".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "solr-collection":
                return new SolrCollectionAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class SolrCollectionAssetManager implements AssetManager {

        SolrDataSource.SolrQueryStepDataSource datasource;

        HttpClient httpClient;
        AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.datasource = buildDataSource(assetDefinition);
            this.assetDefinition = assetDefinition;
            this.httpClient = HttpClient.newBuilder().build();
        }

        @Override
        public boolean assetExists() throws Exception {
            return describeCollection();
        }

        @Override
        public void deployAsset() throws Exception {
            List<Map<String, Object>> statements =
                    (List<Map<String, Object>>)
                            assetDefinition
                                    .getConfig()
                                    .getOrDefault("create-statements", List.of());
            execStatements(statements);
            describeCollection();
        }

        private boolean describeCollection() throws IOException, InterruptedException {
            String collectionUrl = datasource.getRESTCollectionUrl();
            String result = execute("GET", collectionUrl);
            if (result == null) { // this should have been 404, but actually Solr 9.3 returns 400
                log.info("Collection {} does not exist", datasource.getCollectionName());
                return false;
            }
            String schemaUrl = datasource.getCollectionUrl() + "/schema";
            String currentSchema = execute("GET", schemaUrl);
            log.info("Describe collection {}", datasource.getCollectionName());
            log.info("Result: {}", currentSchema);
            return true;
        }

        private void execStatements(List<Map<String, Object>> statements) throws Exception {
            for (Map<String, Object> statement : statements) {
                log.info("Executing: {}", statement);
                String body = (String) statement.getOrDefault("body", "");
                String asJson = body.startsWith("{") ? body : "{" + body + "}";
                String api = (String) statement.get("api");
                String method = (String) statement.get("method");
                if (method == null) {
                    method = "POST";
                }
                String url;
                switch (api) {
                    case "/api/collections":
                        url = datasource.getBaseUrl() + "/api/collections";
                        break;
                    case "/schema":
                        url = datasource.getCollectionUrl() + "/schema";
                        break;
                    default:
                        throw new IllegalStateException("Unexpected api value: " + api);
                }

                HttpResponse<String> response =
                        httpClient.send(
                                HttpRequest.newBuilder()
                                        .uri(URI.create(url))
                                        .method(method, HttpRequest.BodyPublishers.ofString(asJson))
                                        .header("Content-Type", "application/json")
                                        .build(),
                                HttpResponse.BodyHandlers.ofString());
                log.info("Response: {}", response.body());
                if (response.statusCode() != 200) {
                    throw new IOException("Error while executing statement: " + response.body());
                }
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            boolean exists = describeCollection();
            if (!exists) {
                return false;
            }
            String collectionUrl = datasource.getRESTCollectionUrl();
            ;
            log.info("Deleting collection {}", datasource.getCollectionName());
            execute("DELETE", collectionUrl);
            return true;
        }

        private String execute(String method, String url) throws IOException, InterruptedException {
            HttpResponse<String> response =
                    httpClient.send(
                            HttpRequest.newBuilder()
                                    .uri(URI.create(url))
                                    .method(method, HttpRequest.BodyPublishers.noBody())
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            log.info("{} {}: Status code {}", method, url, response.statusCode());
            if (response.statusCode()
                            == 400 // this should have been 404, but actually Solr 9.3 returns 400
                    || response.statusCode() == 404) {
                return null;
            }
            if (response.statusCode() != 200) {
                throw new IOException(
                        "Error while calling url "
                                + url
                                + ", http status code is "
                                + response.statusCode());
            }
            return response.body();
        }

        @Override
        public void close() throws Exception {
            if (datasource != null) {
                datasource.close();
            }
        }
    }

    private static SolrDataSource.SolrQueryStepDataSource buildDataSource(
            AssetDefinition assetDefinition) {
        SolrDataSource dataSource = new SolrDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        SolrDataSource.SolrQueryStepDataSource result =
                dataSource.createDataSourceImplementation(configuration);
        result.initialize(null);
        return result;
    }
}
