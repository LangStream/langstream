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

import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpecDiffer {

    private static final JSONComparator.Result EXPECTED_WAS_NULL_RESULT =
            new JSONComparator.Result() {
                @Override
                public boolean areEquals() {
                    return false;
                }

                @Override
                public List<JSONComparator.FieldComparisonDiff> diffs() {
                    return List.of(
                            new JSONComparator.FieldComparisonDiff(
                                    "ROOT_OBJECT", null, "NOT NULL"));
                }
            };

    private static final JSONComparator.Result ACTUAL_WAS_NULL_RESULT =
            new JSONComparator.Result() {
                @Override
                public boolean areEquals() {
                    return false;
                }

                @Override
                public List<JSONComparator.FieldComparisonDiff> diffs() {
                    return List.of(
                            new JSONComparator.FieldComparisonDiff(
                                    "ROOT_OBJECT", "NOT NULL", null));
                }
            };

    private SpecDiffer() {}

    public static JSONComparator.Result generateDiff(String expectedJson, String actualJson) {
        if (expectedJson == null && actualJson == null) {
            return JSONComparator.RESULT_EQUALS;
        }
        if (expectedJson == null) {
            return EXPECTED_WAS_NULL_RESULT;
        }
        if (actualJson == null) {
            return ACTUAL_WAS_NULL_RESULT;
        }
        return new JSONAssertComparator().compare(expectedJson, actualJson);
    }

    public static JSONComparator.Result generateDiff(Object expectedSpec, Object actualSpec) {
        if (expectedSpec == null && actualSpec == null) {
            return JSONComparator.RESULT_EQUALS;
        }
        if (expectedSpec == null) {
            return EXPECTED_WAS_NULL_RESULT;
        }
        if (actualSpec == null) {
            return ACTUAL_WAS_NULL_RESULT;
        }
        final String expectedStr = SerializationUtil.writeAsJson(expectedSpec);
        final String actualStr = SerializationUtil.writeAsJson(actualSpec);
        return generateDiff(expectedStr, actualStr);
    }

    public static JSONComparator.Result generateDiff(Object expectedSpec, String actualJson) {
        if (expectedSpec == null && actualJson == null) {
            return JSONComparator.RESULT_EQUALS;
        }
        if (expectedSpec == null) {
            return EXPECTED_WAS_NULL_RESULT;
        }
        if (actualJson == null) {
            return ACTUAL_WAS_NULL_RESULT;
        }
        final String expectedStr = SerializationUtil.writeAsJson(expectedSpec);
        return generateDiff(expectedStr, actualJson);
    }

    public static JSONComparator.Result generateDiff(String expectedJson, Object actualSpec) {
        if (expectedJson == null && actualSpec == null) {
            return JSONComparator.RESULT_EQUALS;
        }
        if (expectedJson == null) {
            return ACTUAL_WAS_NULL_RESULT;
        }
        if (actualSpec == null) {
            return EXPECTED_WAS_NULL_RESULT;
        }
        final String actualStr = SerializationUtil.writeAsJson(actualSpec);
        return generateDiff(expectedJson, actualStr);
    }

    public static void logDetailedSpecDiff(JSONComparator.Result diff) {
        logDetailedSpecDiff(diff, null, null);
    }

    public static void logDetailedSpecDiff(
            JSONComparator.Result diff, String currentAsJson, String newSpecAsJson) {
        if (diff == EXPECTED_WAS_NULL_RESULT) {
            log.info("Spec was null");
            return;
        }
        if (diff == ACTUAL_WAS_NULL_RESULT) {
            log.info("Spec is now null");
            return;
        }
        if (log.isDebugEnabled() && (currentAsJson != null || newSpecAsJson != null)) {
            log.debug("logging detailed diff: \nwas: {}\nnow: {}", currentAsJson, newSpecAsJson);
        }
        for (JSONComparator.FieldComparisonDiff failure : diff.diffs()) {
            final String actualValue = failure.actual();
            final String completeField = failure.field();
            final String expectedValue = failure.expected();
            if (actualValue == null) {
                log.info(
                        """
                        was: '%s=%s', now removed
                        """
                                .formatted(completeField, expectedValue));
            } else if (expectedValue == null) {
                log.info(
                        """
                        was empty, now: '%s=%s'
                        """
                                .formatted(completeField, actualValue));
            } else {
                log.info(
                        """
                        '%s' value differs:
                            was: %s
                            now: %s
                        """
                                .formatted(completeField, expectedValue, actualValue));
            }
        }
    }
}
