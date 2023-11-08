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
package ai.langstream.ai.agents.flare;

import static ai.langstream.ai.agents.commons.MutableRecord.mutableRecordToRecord;
import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/** This agents implements the Flare algorithm. */
@Slf4j
public class FlareControllerAgent extends AbstractAgentCode implements AgentProcessor {

    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    private JstlEvaluator<List<String>> tokensAccessor;

    private JstlEvaluator<List<Double>> logsProbsAccessor;

    private JstlEvaluator<Integer> numIterationsAccessor;

    private double minProb;
    private int minTokenGap;
    private int numPadTokens;
    private int maxIterations;

    private String loopTopic;
    private String retrieveDocumentsField;
    private String numIterationsField;

    private TopicProducer loopTopicProducer;

    @Override
    public void init(Map<String, Object> configuration) {
        String tokensField = ConfigurationUtils.getString("tokens-field", "", configuration);
        String logProbsField = ConfigurationUtils.getString("logprobs-field", "", configuration);
        tokensAccessor = new JstlEvaluator("${" + tokensField + "}", List.class);
        logsProbsAccessor = new JstlEvaluator("${" + logProbsField + "}", List.class);
        loopTopic = ConfigurationUtils.getString("loop-topic", "", configuration);
        retrieveDocumentsField =
                ConfigurationUtils.getString(
                        "loop-topic", "retrieve-documents-field ", configuration);

        // Minimum probability for a token to be considered low confidence.
        minProb = ConfigurationUtils.getDouble("min-prob", 0.2, configuration);
        // Minimum number of tokens between two low confidence spans.
        minTokenGap = ConfigurationUtils.getInt("min-token-gap", 5, configuration);
        //  Number of tokens to pad around a low confidence span
        numPadTokens = ConfigurationUtils.getInt("num-pad-tokens", 2, configuration);
        maxIterations = ConfigurationUtils.getInt("max-iterations", 10, configuration);
        numIterationsField =
                ConfigurationUtils.getString(
                        "num-iterations-field", "value.flare_iterations", configuration);
        numIterationsAccessor = new JstlEvaluator<>("${" + numIterationsField + "}", Integer.class);
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        this.loopTopicProducer =
                context.getTopicConnectionProvider()
                        .createProducer(context.getGlobalAgentId(), loopTopic, Map.of());
    }

    @Override
    public void start() throws Exception {
        this.loopTopicProducer.start();
    }

    @Override
    public void close() throws Exception {
        if (this.loopTopicProducer != null) {
            this.loopTopicProducer.close();
        }
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record record : records) {
            processRecord(record, recordSink);
        }
    }

    public void processRecord(Record record, RecordSink recordSink) {
        if (record == null) {
            recordSink.emitEmptyList(record);
            return;
        }
        MutableRecord mutableRecord = recordToMutableRecord(record, true).copy();

        Integer iterationCount = numIterationsAccessor.evaluate(mutableRecord);
        if (iterationCount == null) {
            iterationCount = 0;
        }
        if (iterationCount > maxIterations) {
            log.info("The record did too many iterations - {}, stopping", iterationCount);
            recordSink.emitSingleResult(record, record);
            return;
        } else {
            log.info("Record did {} iterations in Flare controller", iterationCount);
        }
        List<String> tokens = tokensAccessor.evaluate(mutableRecord);
        log.info("Flare: tokens: {}", tokens);
        List<Double> logProbs = logsProbsAccessor.evaluate(mutableRecord);

        List<String> logConfidenceSpans =
                lowConfidenceSpans(tokens, logProbs, minProb, minTokenGap, numPadTokens);
        logConfidenceSpans.forEach(
                span -> {
                    log.info("Flare: low confidence span: {}", span);
                });

        if (logConfidenceSpans.isEmpty()) {
            log.info("The record is good, sending it to the next agent");
            // record is good, send it to the next agent
            recordSink.emitSingleResult(record, record);
            return;
        }
        log.info("The LLM needs more content about these spans: {}", logConfidenceSpans);

        mutableRecord.setResultField(
                logConfidenceSpans,
                retrieveDocumentsField,
                Schema.createArray(Schema.create(Schema.Type.STRING)),
                avroKeySchemaCache,
                avroValueSchemaCache);

        Record forLoopTopic = mutableRecordToRecord(mutableRecord).orElseThrow();

        loopTopicProducer
                .write(forLoopTopic)
                .whenComplete(
                        (__, error) -> {
                            if (error != null) {
                                recordSink.emitError(record, error);
                            } else {
                                // the old record is completed
                                // control is now passed into the "loopTopic"
                                recordSink.emitEmptyList(record);
                            }
                        });
    }

    private static List<String> lowConfidenceSpans(
            List<String> tokens,
            List<Double> logProbs,
            double minProb,
            int minTokenGap,
            int numPadTokens) {
        List<Integer> lowIdx = new ArrayList<>();
        for (int i = 0; i < logProbs.size(); i++) {
            if (Math.exp(logProbs.get(i)) < minProb && tokens.get(i).matches("\\w")) {
                lowIdx.add(i);
            }
        }

        if (lowIdx.isEmpty()) {
            return new ArrayList<>();
        }

        List<int[]> spans = new ArrayList<>();
        spans.add(new int[] {lowIdx.get(0), lowIdx.get(0) + numPadTokens + 1});

        for (int i = 1; i < lowIdx.size(); i++) {
            int idx = lowIdx.get(i);
            int end = idx + numPadTokens + 1;

            if (idx - lowIdx.get(i - 1) < minTokenGap) {
                spans.get(spans.size() - 1)[1] = end;
            } else {
                spans.add(new int[] {idx, end});
            }
        }

        List<String> result = new ArrayList<>();
        for (int[] span : spans) {
            StringBuilder spanBuilder = new StringBuilder();
            for (int j = span[0]; j < span[1]; j++) {
                spanBuilder.append(tokens.get(j));
            }
            if (!spanBuilder.isEmpty()) {
                result.add(spanBuilder.toString());
            }
        }

        return result;
    }

    public static class FlareControllerAgentCodeProvider implements AgentCodeProvider {

        private static final Set<String> STEP_TYPES = Set.of("flare-controller");

        @Override
        public boolean supports(String agentType) {
            return STEP_TYPES.contains(agentType);
        }

        @Override
        public AgentCode createInstance(String agentType) {
            return new FlareControllerAgent();
        }
    }
}
