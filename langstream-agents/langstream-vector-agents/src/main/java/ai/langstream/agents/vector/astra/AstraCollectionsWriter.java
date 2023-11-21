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
package ai.langstream.agents.vector.astra;

import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraCollectionsWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "astra-collections".equals(dataSourceConfig.get("service"));
    }

    @Override
    public VectorDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new AstraCollectionsDatabaseWriter(datasourceConfig);
    }

    private static class AstraCollectionsDatabaseWriter implements VectorDatabaseWriter {

        private final Map<String, Object> datasourceConfig;

        public AstraCollectionsDatabaseWriter(Map<String, Object> datasourceConfig) {
            this.datasourceConfig = datasourceConfig;
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) {}

        @Override
        public CompletableFuture<?> upsert(Record record, Map<String, Object> context) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public void close() {}
    }
}
