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
package ai.langstream.agents.vector;

import static ai.langstream.ai.agents.commons.MutableRecord.mutableRecordToRecord;
import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.predicate.JstlPredicate;
import ai.langstream.ai.agents.datasource.DataSourceProviderRegistry;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import com.datastax.oss.streaming.ai.QueryStep;
import com.datastax.oss.streaming.ai.StepPredicatePair;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.QueryConfig;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryVectorDBAgent extends SingleRecordAgentProcessor {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private QueryStepDataSource dataSource;
    private QueryStep queryExecutor;

    private Collection<StepPredicatePair> steps;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {

        Map<String, Object> datasourceConfiguration =
                (Map<String, Object>) configuration.get("datasource");
        dataSource = DataSourceProviderRegistry.getQueryStepDataSource(datasourceConfiguration);
        dataSource.initialize(datasourceConfiguration);

        configuration.put("type", "query");
        QueryConfig queryConfig = MAPPER.convertValue(configuration, QueryConfig.class);
        queryExecutor = (QueryStep) TransformFunctionUtil.newQuery(queryConfig, dataSource);
        JstlPredicate when =
                queryConfig.getWhen() == null ? null : new JstlPredicate(queryConfig.getWhen());
        steps = List.of(new StepPredicatePair(queryExecutor, when));
    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Processing record {}", record);
        }
        MutableRecord context = recordToMutableRecord(record, true);
        TransformFunctionUtil.processTransformSteps(context, steps);
        context.convertMapToStringOrBytes();
        Optional<Record> recordResult = mutableRecordToRecord(context);
        if (log.isDebugEnabled()) {
            log.debug("recordResult {}", recordResult);
        }
        return recordResult.map(List::of).orElseGet(List::of);
    }

    @Override
    public void start() {}

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
