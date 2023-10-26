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

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.util.ConfigurationUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

@Slf4j
public class SolrWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "solr".equals(dataSourceConfig.get("service"));
    }

    @Override
    public SolrVectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new SolrVectorDatabaseWriter(datasourceConfig);
    }

    public static class SolrVectorDatabaseWriter implements VectorDatabaseWriter, AutoCloseable {

        private final SolrDataSource.SolrQueryStepDataSource dataSource;

        private Map<String, JstlEvaluator> fields = new HashMap<>();

        private Http2SolrClient client;

        private int commitWithin = 1000;

        public SolrVectorDatabaseWriter(Map<String, Object> datasourceConfig) {
            SolrDataSource dataSourceProvider = new SolrDataSource();
            dataSource = dataSourceProvider.createDataSourceImplementation(datasourceConfig);
        }

        @Override
        public void close() throws Exception {
            dataSource.close();
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) throws Exception {
            commitWithin = ConfigurationUtils.getInt("commit-within", 1000, agentConfiguration);
            log.info("Commit within {}", commitWithin);
            if (commitWithin <= 0) {
                log.warn(
                        "Commit within is set to 0, this may cause performance issues, as each record will be committed separately");
            }

            List<Map<String, Object>> fields =
                    (List<Map<String, Object>>)
                            agentConfiguration.getOrDefault("fields", List.of());
            fields.forEach(
                    field -> {
                        this.fields.put(
                                field.get("name").toString(),
                                buildEvaluator(field, "expression", Object.class));
                    });
            dataSource.initialize(null);

            client = dataSource.getClient();
        }

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {
            CompletableFuture<?> handle = new CompletableFuture<>();
            try {
                MutableRecord mutableRecord = recordToMutableRecord(record, true);

                SolrInputDocument document = new SolrInputDocument();
                fields.forEach(
                        (name, evaluator) -> {
                            Object value = evaluator.evaluate(mutableRecord);
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "setting value {} ({}) for field {}",
                                        value,
                                        value.getClass(),
                                        name);
                            }
                            document.addField(name, value);
                        });

                if (record.value() != null) {
                    UpdateResponse response = client.add(document, commitWithin);
                    if (log.isDebugEnabled()) {
                        log.debug("Result {}", response);
                    }
                    if (response.getException() != null) {
                        handle.completeExceptionally(response.getException());
                    } else {
                        commitIfNeeded();
                        handle.complete(null);
                    }
                } else {
                    SolrInputField id = document.get("id");
                    if (id == null) {
                        throw new IllegalStateException(
                                "In order to perform DELETE (value=nul) you must provide id field");
                    }
                    UpdateResponse response =
                            client.deleteById((String) id.getValue(), commitWithin);
                    if (log.isDebugEnabled()) {
                        log.debug("Result {}", response);
                    }
                    if (response.getException() != null) {
                        handle.completeExceptionally(response.getException());
                    } else {
                        commitIfNeeded();
                        handle.complete(null);
                    }
                }
            } catch (Exception e) {
                handle.completeExceptionally(e);
            }
            return handle;
        }

        private void commitIfNeeded() throws SolrServerException, IOException {
            if (commitWithin <= 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Force commit");
                }
                client.commit();
            }
        }
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
