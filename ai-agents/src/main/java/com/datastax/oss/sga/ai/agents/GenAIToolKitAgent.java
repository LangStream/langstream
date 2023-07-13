package com.datastax.oss.sga.ai.agents;

import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.getTransformSteps;
import com.azure.ai.openai.OpenAIClient;
import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.jstl.predicate.StepPredicatePair;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class GenAIToolKitAgent implements AgentCode  {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private List<StepPredicatePair> steps;
    private TransformStepConfig config;
    private QueryStepDataSource dataSource;

    @Override
    public List<Record> process(List<Record> records) throws Exception {
        log.info("Processing {}", records);
        List<Record> output = new ArrayList<>();
        for (Record record : records) {
            TransformContext context = recordToTransformContext(record);
            if (config.isAttemptJsonConversion()) {
                context.setKeyObject(
                    TransformFunctionUtil.attemptJsonConversion(context.getKeyObject()));
                context.setValueObject(
                    TransformFunctionUtil.attemptJsonConversion(context.getValueObject()));
            }
            TransformFunctionUtil.processTransformSteps(context, steps);

            transformContextToRecord(context).ifPresent(output::add);
        }
        return output;
    }

    @Override
    public void init(Map<String, Object> configuration) {
        config = MAPPER.convertValue(configuration, TransformStepConfig.class);
        OpenAIClient openAIClient = TransformFunctionUtil.buildOpenAIClient(config.getOpenai());
        dataSource = TransformFunctionUtil.buildDataSource(config.getDatasource());
        steps = TransformFunctionUtil.getTransformSteps(config, openAIClient, dataSource);
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        for (StepPredicatePair pair : steps) {
            pair.getTransformStep().close();
        }
    }

    private TransformContext recordToTransformContext(Record record) {
        TransformContext context = new TransformContext();
        // TODO: add support for key and value schemas
        context.setKeyObject(record.key());
        context.setValueObject(record.value());
        return context;
    }

    private Optional<Record> transformContextToRecord(TransformContext context) {
        if (context.isDropCurrentRecord()) {
            return Optional.empty();
        }
        // TODO: add support for key and value schemas
        return Optional.of(new TransformRecord(context.getKeyObject(), context.getValueObject()));
    }

    private record TransformRecord(Object key, Object value) implements Record {
    }
}
