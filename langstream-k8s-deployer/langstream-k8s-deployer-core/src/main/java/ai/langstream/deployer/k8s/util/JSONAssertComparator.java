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
package ai.langstream.deployer.k8s.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.skyscreamer.jsonassert.JSONParser;
import org.skyscreamer.jsonassert.comparator.DefaultComparator;

public class JSONAssertComparator implements JSONComparator {

    @Override
    @SneakyThrows
    public Result compare(String expectedStr, String actualStr) {

        final DefaultComparatorNoGenericFailures comparator =
                new DefaultComparatorNoGenericFailures();

        Object expected = JSONParser.parseJSON(expectedStr);
        Object actual = JSONParser.parseJSON(actualStr);
        final JSONCompareResult jsonCompareResult = new NoGenericFailuresCompareResult();
        if ((expected instanceof JSONObject) && (actual instanceof JSONObject)) {
            comparator.compareJSON(
                    "", (JSONObject) expected, (JSONObject) actual, jsonCompareResult);
        } else if ((expected instanceof JSONArray) && (actual instanceof JSONArray)) {
            comparator.compareJSONArray(
                    "", (JSONArray) expected, (JSONArray) actual, jsonCompareResult);
        } else {
            throw new IllegalArgumentException();
        }

        return new ResultWrapper(jsonCompareResult, expectedStr, actualStr);
    }

    private static class NoGenericFailuresCompareResult extends JSONCompareResult {
        int lastFailCallNumOfFailures;

        @Override
        public void fail(String message) {
            int currentFieldFailures =
                    getFieldFailures().size()
                            + getFieldUnexpected().size()
                            + getFieldMissing().size();
            if (currentFieldFailures == lastFailCallNumOfFailures) {
                throw new IllegalArgumentException(
                        "JSONCompareResult.fail() should not be called without the field details");
            }
            lastFailCallNumOfFailures++;
            super.fail(message);
        }
    }

    private static class DefaultComparatorNoGenericFailures extends DefaultComparator {
        public DefaultComparatorNoGenericFailures() {
            super(JSONCompareMode.STRICT);
        }

        @Override
        public void compareJSONArray(
                String prefix, JSONArray expected, JSONArray actual, JSONCompareResult result)
                throws JSONException {
            if (expected.length() != actual.length()) {
                result.fail(prefix, expected, actual);
                return;
            }
            super.compareJSONArray(prefix, expected, actual, result);
        }
    }

    @AllArgsConstructor
    private static class ResultWrapper implements Result {
        private final JSONCompareResult jsonCompareResult;
        private final String expected;
        private final String actual;

        @Override
        public boolean areEquals() {
            return jsonCompareResult.passed();
        }

        @Override
        public List<FieldComparisonDiff> diffs() {
            if (jsonCompareResult.passed()) {
                return Collections.emptyList();
            }

            final Map<String, Object> expectedAsMap =
                    SerializationUtil.readJson(expected, Map.class);
            final Map<String, Object> actualAsMap = SerializationUtil.readJson(actual, Map.class);

            return Stream.of(
                            jsonCompareResult.getFieldFailures(),
                            jsonCompareResult.getFieldMissing(),
                            jsonCompareResult.getFieldUnexpected())
                    .flatMap(Collection::stream)
                    .map(f -> toFieldComparisonFailure(f, expectedAsMap, actualAsMap))
                    .collect(Collectors.toList());
        }
    }

    private static FieldComparisonDiff toFieldComparisonFailure(
            org.skyscreamer.jsonassert.FieldComparisonFailure fieldFailure,
            Map<String, Object> expectedAsMap,
            Map<String, Object> actualAsMap) {
        final Object actual = fieldFailure.getActual();
        final Object expected = fieldFailure.getExpected();
        final String field = fieldFailure.getField();

        if (expected == null && actual == null) {
            throw new IllegalStateException();
        }

        if (actual == null) {
            final String quotedExpected = "\"" + expected + "\"";
            final String completeField = "%s.%s".formatted(field, quotedExpected);
            final String valueByDotNotation = getValueByDotNotation(expectedAsMap, completeField);
            return new FieldComparisonDiff(completeField, valueByDotNotation, null);
        } else if (expected == null) {
            final Object quotedActual = "\"" + actual + "\"";
            final String completeField = "%s.%s".formatted(field, quotedActual);
            final String valueByDotNotation = getValueByDotNotation(actualAsMap, completeField);
            return new FieldComparisonDiff(completeField, null, valueByDotNotation);
        } else {
            return new FieldComparisonDiff(field, expected.toString(), actual.toString());
        }
    }

    private static String getValueByDotNotation(final Map<String, Object> map, String key) {
        if (key.startsWith("data.")) {
            // ConfigMap data, need to handle nested maps with key with dots
            final Map<String, Object> data = (Map<String, Object>) map.get("data");
            if (data != null) {
                for (String dataKey : data.keySet()) {
                    key = key.replace("data." + dataKey, "data.\"" + dataKey + "\"");
                }
            }
        }
        List<String> words = new ArrayList<>();
        StringBuilder currentWord = new StringBuilder();
        boolean openQuote = false;
        for (char c : key.toCharArray()) {
            if (c == '"') {
                openQuote = !openQuote;
                continue;
            }
            if (c == '.') {
                if (!openQuote) {
                    if (currentWord.length() > 0) {
                        words.add(currentWord.toString());
                    }
                    currentWord = new StringBuilder();
                    continue;
                }
            }
            currentWord.append(c);
        }
        words.add(currentWord.toString());
        Map<String, Object> currentMap = map;
        for (int i = 0; i < words.size() - 1; i++) {
            final String word = words.get(i);
            if (word.endsWith("]")) {
                int index =
                        Integer.parseInt(word.substring(word.indexOf("[") + 1, word.indexOf("]")));
                final List<Object> list =
                        (List<Object>) currentMap.get(word.replace("[" + index + "]", ""));
                currentMap = (Map<String, Object>) list.get(index);
            } else {
                currentMap = (Map<String, Object>) currentMap.get(word);
            }
        }

        return currentMap.get(words.get(words.size() - 1)).toString();
    }
}
