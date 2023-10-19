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

import static ai.langstream.ai.agents.commons.MutableRecord.mutableRecordToRecord;
import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class ReRankAgent extends SingleRecordAgentProcessor {

    public static final String ALGORITHM_NONE = "none";
    public static final String ALGORITHM_MMR = "MMR";

    private String outputField;
    private String algorithm = "none";

    private int max;

    private double mmr_lambda;

    // BM25 parameters
    private double bm25_k1;
    private double bm25_b;

    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    private JstlEvaluator<Object> fieldAccessor;

    private JstlEvaluator<String> queryAccessor;

    private JstlEvaluator<Object> queryEmbeddingsAccessor;

    private JstlEvaluator<String> recordTextAccessor;
    private JstlEvaluator<Object> recordEmbeddingsAccessor;

    @Override
    public void init(Map<String, Object> configuration) {
        String field =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "field", () -> "re-rank agent");

        fieldAccessor = new JstlEvaluator<>("${" + field + "}", List.class);
        outputField =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "output-field", () -> "re-rank agent");
        algorithm = ConfigurationUtils.getString("algorithm", ALGORITHM_NONE, configuration);

        String queryEmbeddingsField =
                ConfigurationUtils.getString("query-embeddings", "", configuration);
        String queryField = ConfigurationUtils.getString("query-text", "", configuration);
        String embeddingsInRecord =
                ConfigurationUtils.getString("embeddings-field", "", configuration);
        String textInRecord = ConfigurationUtils.getString("text-field", "", configuration);
        max = ConfigurationUtils.getInt("max", 100, configuration);
        mmr_lambda = ConfigurationUtils.getDouble("lambda", 0.5, configuration);
        bm25_k1 = ConfigurationUtils.getDouble("k1", 1.5, configuration);
        bm25_b = ConfigurationUtils.getDouble("b", 0.75, configuration);

        queryEmbeddingsAccessor =
                new JstlEvaluator<>("${" + queryEmbeddingsField + "}", List.class);
        queryAccessor = new JstlEvaluator<>("${" + queryField + "}", String.class);
        recordEmbeddingsAccessor = new JstlEvaluator<>("${" + embeddingsInRecord + "}", List.class);
        recordTextAccessor = new JstlEvaluator<>("${" + textInRecord + "}", String.class);
    }

    record TextWithEmbeddings(String text, float[] embeddings) {}

    @Override
    public List<Record> processRecord(Record record) throws Exception {
        if (record == null) {
            return List.of();
        }
        MutableRecord mutableRecord = recordToMutableRecord(record, true).copy();

        List<Object> currentList = (List<Object>) fieldAccessor.evaluate(mutableRecord);

        TextWithEmbeddings query =
                new TextWithEmbeddings(
                        queryAccessor.evaluate(mutableRecord),
                        toArrayOfFloat(queryEmbeddingsAccessor.evaluate(mutableRecord)));

        Function<Object, TextWithEmbeddings> recordExtractor =
                (Object o) -> {
                    MutableRecord context = new MutableRecord();
                    context.setRecordObject(o);
                    String text = recordTextAccessor.evaluate(context);
                    float[] embeddings = toArrayOfFloat(recordEmbeddingsAccessor.evaluate(context));
                    if (embeddings == null) {
                        throw new IllegalArgumentException("Embeddings are null in record: " + o);
                    }
                    if (text == null) {
                        throw new IllegalArgumentException("Text is null in record: " + o);
                    }
                    return new TextWithEmbeddings(text, embeddings);
                };
        List<Object> result = rerank(currentList, query, recordExtractor);
        mutableRecord.setResultField(
                result, outputField, null, avroKeySchemaCache, avroValueSchemaCache);
        mutableRecord.convertMapToStringOrBytes();
        Optional<Record> recordResult = mutableRecordToRecord(mutableRecord);
        return recordResult.map(List::of).orElse(List.of());
    }

    private List<Object> rerank(
            List<Object> currentList,
            TextWithEmbeddings query,
            Function<Object, TextWithEmbeddings> recordExtractor) {

        if (query.text == null || query.text.isEmpty()) {
            return new ArrayList<>(currentList);
        }

        switch (algorithm) {
            case ALGORITHM_MMR:
                return rerankMMR(
                        currentList, max, query, mmr_lambda, bm25_k1, bm25_b, recordExtractor);
            case ALGORITHM_NONE:
                return new ArrayList<>(currentList);
            default:
                throw new IllegalStateException();
        }
    }

    private static List<Object> rerankMMR(
            List<Object> documents,
            int max,
            TextWithEmbeddings query,
            double lambda,
            double bm25_k1,
            double bm25_b,
            Function<Object, TextWithEmbeddings> recordExtractor) {
        List<Object> rankedDocuments = new ArrayList<>();
        List<Object> remainingDocuments = new ArrayList<>(documents);

        while (!remainingDocuments.isEmpty() && rankedDocuments.size() < max) {
            Object topDocument =
                    calculateTopDocumentMMR_BM25_CosineSimilarity(
                            remainingDocuments,
                            rankedDocuments,
                            recordExtractor,
                            query,
                            lambda,
                            bm25_k1,
                            bm25_b);
            rankedDocuments.add(topDocument);
            remainingDocuments.remove(topDocument);
        }

        return rankedDocuments;
    }

    private static Object calculateTopDocumentMMR_BM25_CosineSimilarity(
            List<Object> remainingDocuments,
            List<Object> rankedDocuments,
            Function<Object, TextWithEmbeddings> recordExtractor,
            TextWithEmbeddings query,
            double lambda,
            double bm25_k1,
            double bm25_b) {
        Object topDocument = null;
        double topScore = Double.NEGATIVE_INFINITY;

        List<TextWithEmbeddings> texts =
                remainingDocuments.stream()
                        .map(recordExtractor)
                        .filter(
                                t ->
                                        t.text != null
                                                && t.embeddings != null
                                                && !t.text.isEmpty()
                                                && t.embeddings.length > 0)
                        .toList();

        double[] bm25scores = calculateBM25Scores(texts, query, bm25_k1, bm25_b);

        for (int i = 0; i < texts.size(); i++) {
            TextWithEmbeddings document = texts.get(i);
            double relevance = bm25scores[i];
            double diversity = calculateDiversity(document, rankedDocuments, recordExtractor);
            double score = lambda * relevance - (1 - lambda) * diversity;
            if (score > topScore) {
                topScore = score;
                topDocument = remainingDocuments.get(i);
            }
        }

        if (topDocument == null) {
            throw new IllegalStateException("topDocument is null, among " + remainingDocuments);
        }

        return topDocument;
    }

    private static double calculateDiversity(
            TextWithEmbeddings document,
            List<Object> rankedDocuments,
            Function<Object, TextWithEmbeddings> textExtractor) {
        if (rankedDocuments.isEmpty()) {
            // there is no document, the diversity is 0
            return 0;
        }

        double sumCosineSimilarity = 0.0;
        float[] embeddings = document.embeddings;

        // Convert embeddings to RealVector objects for cosine similarity calculation

        for (Object otherDocument : rankedDocuments) {
            TextWithEmbeddings text = textExtractor.apply(otherDocument);
            float[] otherVector = text.embeddings;

            // Calculate cosine similarity between target and other embeddings
            double cosineSimilarity = cosineSimilarity(embeddings, otherVector);

            // Add the cosine similarity to the sum
            sumCosineSimilarity += cosineSimilarity;
        }

        // Calculate the average cosine similarity as the diversity score
        return sumCosineSimilarity / rankedDocuments.size();
    }

    private static float euclideanNorm(float[] arr) {
        float sumOfSquares = 0.0f;
        for (float value : arr) {
            sumOfSquares += value * value;
        }
        return (float) Math.sqrt(sumOfSquares);
    }

    public static float cosineSimilarity(float[] arr1, float[] arr2) {
        float norm1 = euclideanNorm(arr1);
        if (norm1 == 0) {
            return 0;
        }
        float norm2 = euclideanNorm(arr2);
        if (norm2 == 0) {
            return 0.0f;
        }
        float dotProduct = dotProduct(arr1, arr2);
        return dotProduct / (norm1 * norm2);
    }

    public static double[] calculateBM25Scores(
            List<TextWithEmbeddings> documents, TextWithEmbeddings query, double k1, double b) {
        int N = documents.size(); // Total number of documents
        double avgdl = calculateAverageDocumentLength(documents);
        Map<String, Integer>[] documentTermFrequencies =
                calculateDocumentTermFrequencies(documents);

        String[] queryTerms = tokenise(query.text);
        double[] bm25Scores = new double[N];

        for (int i = 0; i < N; i++) {
            String text = documents.get(i).text;
            String[] documentTerms = tokenise(text);
            double documentLength = documentTerms.length;

            for (String term : queryTerms) {
                int tf = documentTermFrequencies[i].getOrDefault(term, 0);
                double idf = calculateIDF(term, documentTermFrequencies, N);

                double numerator = tf * (k1 + 1);
                double denominator = tf + k1 * (1 - b + b * (documentLength / avgdl));

                bm25Scores[i] += idf * (numerator / denominator);
            }
        }

        return bm25Scores;
    }

    public static double calculateAverageDocumentLength(List<TextWithEmbeddings> documents) {
        int totalTerms = 0;
        for (TextWithEmbeddings document : documents) {
            String text = document.text;
            String[] terms = tokenise(text);
            totalTerms += terms.length;
        }
        return (double) totalTerms / documents.size();
    }

    private static String[] tokenise(String text) {
        return text.split("\\s+");
    }

    public static Map<String, Integer>[] calculateDocumentTermFrequencies(
            List<TextWithEmbeddings> documents) {
        Map<String, Integer>[] termFrequencies = new Map[documents.size()];

        for (int i = 0; i < documents.size(); i++) {
            termFrequencies[i] = new HashMap<>();
            String text = documents.get(i).text;
            String[] terms = tokenise(text);
            for (String term : terms) {
                termFrequencies[i].put(term, termFrequencies[i].getOrDefault(term, 0) + 1);
            }
        }

        return termFrequencies;
    }

    public static double calculateIDF(
            String term, Map<String, Integer>[] documentTermFrequencies, int N) {
        int documentCountWithTerm = 0;
        for (Map<String, Integer> termFrequency : documentTermFrequencies) {
            if (termFrequency.containsKey(term)) {
                documentCountWithTerm++;
            }
        }
        return Math.log((N - documentCountWithTerm + 0.5) / (documentCountWithTerm + 0.5) + 1.0);
    }

    public static float[] toArrayOfFloat(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof Collection<?> collection) {
            float[] result = new float[collection.size()];
            int i = 0;
            for (Object o : collection) {
                result[i++] = coerceToFloat(o);
            }
            return result;
        } else {
            throw new IllegalArgumentException("Cannot convert " + input + " to list of float");
        }
    }

    private static float dotProduct(float[] arr1, float[] arr2) {
        if (arr1.length != arr2.length) {
            throw new IllegalArgumentException("Arrays must have the same length");
        }

        float result = 0.0f;
        for (int i = 0; i < arr1.length; i++) {
            result += arr1[i] * arr2[i];
        }

        return result;
    }

    private static float coerceToFloat(Object o) {
        if (o instanceof Number n) {
            return n.floatValue();
        } else {
            throw new IllegalArgumentException("Cannot convert " + o + " to float");
        }
    }
}
