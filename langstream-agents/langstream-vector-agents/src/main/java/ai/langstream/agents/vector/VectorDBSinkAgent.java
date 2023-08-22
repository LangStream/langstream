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
package ai.langstream.agents.vector;

import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.database.VectorDatabaseWriterProviderRegistry;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.Record;

import java.util.List;
import java.util.Map;

public class VectorDBSinkAgent extends AbstractAgentCode implements AgentSink {

    private VectorDatabaseWriter writer;
    private CommitCallback callback;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        Map<String, Object> datasourceConfiguration = (Map<String, Object>) configuration.get("datasource");
        writer = VectorDatabaseWriterProviderRegistry.createWriter(datasourceConfiguration);
        writer.initialise(configuration);
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void write(List<Record> records) throws Exception {

        // naive implementation, no batching
        Map<String, Object> context = Map.of();
        for (Record record : records) {
            writer.upsert(record, context);
            callback.commit(List.of(record));
        }
    }

    @Override
    public void setCommitCallback(CommitCallback callback) {
        this.callback = callback;
    }
}
