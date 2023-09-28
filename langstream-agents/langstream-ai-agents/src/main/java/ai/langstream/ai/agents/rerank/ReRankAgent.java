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
package ai.langstream.ai.agents.rerank;

import static ai.langstream.ai.agents.GenAIToolKitAgent.transformContextToRecord;

import ai.langstream.ai.agents.GenAIToolKitAgent;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.jstl.JstlEvaluator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class ReRankAgent extends SingleRecordAgentProcessor {

    public static final String ALGORITHM_NONE = "none";
    public static final String ALGORITHM_MMR = "MMR";

    private String field;
    private String outputField;
    private String algorithm = "none";
    private String query;

    private String fieldInRecord;

    private double lambda;

    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    @Override
    public void init(Map<String, Object> configuration) {
        field =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "field", () -> "re-rank agent");
        outputField =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "output-field", () -> "re-rank agent");
        algorithm = ConfigurationUtils.getString("algorithm", ALGORITHM_NONE, configuration);
        query = ConfigurationUtils.getString("query", "", configuration);
        fieldInRecord = ConfigurationUtils.getString("field-in-record", "text", configuration);

        lambda = ConfigurationUtils.getDouble("lambda", 0.5, configuration);
    }

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        TransformContext transformContext =
                GenAIToolKitAgent.recordToTransformContext(record, true).copy();
        JstlEvaluator<List<Object>> fieldAccessor =
                new JstlEvaluator("${" + field + "}", List.class);
        List<Object> currentList = fieldAccessor.evaluate(transformContext);
        Function<Object, String> textExtractor =
                (Object o) -> {
                    if (o instanceof String) {
                        return (String) o;
                    } else if (o instanceof Map) {
                        return ((Map<String, Object>) o).getOrDefault(fieldInRecord, "").toString();
                    } else {
                        return o.toString();
                    }
                };
        List<Object> result = rerank(currentList, textExtractor);
        transformContext.setResultField(
                result, outputField, null, avroKeySchemaCache, avroValueSchemaCache);
        transformContext.convertMapToStringOrBytes();
        Optional<Record> recordResult = transformContextToRecord(transformContext);
        return recordResult.map(List::of).orElse(List.of());
    }

    private List<Object> rerank(List<Object> currentList, Function<Object, String> textExtractor) {
        switch (algorithm) {
            case ALGORITHM_MMR:
                return rerankMMR(currentList, query, lambda, textExtractor);
            case ALGORITHM_NONE:
                return currentList;
            default:
                throw new IllegalStateException();
        }
    }

    private static List<Object> rerankMMR(
            List<Object> documents,
            String query,
            double lambda,
            Function<Object, String> textExtractor) {
        List<Object> rankedDocuments = new ArrayList<>();
        List<Object> remainingDocuments = new ArrayList<>(documents);

        while (!remainingDocuments.isEmpty()) {
            log.info("Remaining documents: {}", remainingDocuments);
            Object topDocument =
                    calculateTopDocumentMMR(
                            remainingDocuments, rankedDocuments, textExtractor, query, lambda);
            rankedDocuments.add(topDocument);
            log.info("Removing top document: {}", topDocument);
            remainingDocuments.remove(topDocument);
        }

        return rankedDocuments;
    }

    public static Object calculateTopDocumentMMR(
            List<Object> remainingDocuments,
            List<Object> rankedDocuments,
            Function<Object, String> textExtractor,
            String query,
            double lambda) {
        Object topDocument = null;
        double topScore = Double.NEGATIVE_INFINITY;

        for (Object documentObject : remainingDocuments) {
            String document = textExtractor.apply(documentObject);
            double relevance = calculateRelevance(document, query);
            double diversity = calculateDiversity(document, rankedDocuments, textExtractor);
            double score = lambda * relevance - (1 - lambda) * diversity;

            if (score > topScore) {
                topScore = score;
                topDocument = documentObject;
            }
        }

        return topDocument;
    }

    public static double calculateRelevance(String document, String query) {
        // Implement your relevance calculation logic here
        // For example, you can use TF-IDF, BM25, or other relevance metrics.
        return document.hashCode()
                * 1.0
                / query.hashCode(); // Placeholder, replace with actual relevance score.
    }

    public static double calculateDiversity(
            String document, List<Object> rankedDocuments, Function<Object, String> textExtractor) {
        // Implement your diversity calculation logic here
        // For example, you can use cosine similarity, Jaccard index, or other diversity metrics.
        return new Random().nextDouble(); // Placeholder, replace with actual diversity score.
    }
}
