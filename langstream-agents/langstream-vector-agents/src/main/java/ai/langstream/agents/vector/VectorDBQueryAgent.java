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

import ai.langstream.ai.agents.GenAIToolKitAgent;
import ai.langstream.ai.agents.datasource.DataSourceProviderRegistry;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import com.datastax.oss.streaming.ai.QueryStep;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.jstl.predicate.StepPredicatePair;
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
import com.datastax.oss.streaming.ai.model.config.QueryConfig;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VectorDBQueryAgent extends SingleRecordAgentProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private QueryStepDataSource dataSource;
    private QueryStep queryExecutor;

    private Collection<StepPredicatePair> steps;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {

        Map<String, Object> datasourceConfiguration = (Map<String, Object>) configuration.get("datasource");
        dataSource = DataSourceProviderRegistry.getQueryStepDataSource(datasourceConfiguration);
        DataSourceConfig dataSourceConfig = MAPPER.convertValue(datasourceConfiguration, DataSourceConfig.class);
        dataSource.initialize(dataSourceConfig);

        configuration.put("type", "query");
        QueryConfig queryConfig = MAPPER.convertValue(configuration, QueryConfig.class);
        queryExecutor = (QueryStep) TransformFunctionUtil.newQuery(queryConfig, dataSource);
        steps = List.of(new StepPredicatePair(queryExecutor, it -> true));
    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        TransformContext context = GenAIToolKitAgent.recordToTransformContext(record, true);
        TransformFunctionUtil.processTransformSteps(context, steps);
        context.convertMapToStringOrBytes();
        Optional<Record> recordResult = GenAIToolKitAgent.transformContextToRecord(context, record.headers());
        return recordResult.isPresent() ? List.of(recordResult.get()) : List.of();
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void close() throws Exception {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
