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

public interface JSONComparator {

    Result RESULT_EQUALS = new Result() {
        @Override
        public boolean areEquals() {
            return true;
        }

        @Override
        public List<FieldComparisonDiff> diffs() {
            return null;
        }
    };

    record FieldComparisonDiff(String field, String expected, String actual) {
    }

    interface Result {
        boolean areEquals();

        List<FieldComparisonDiff> diffs();
    }

    Result compare(String json1, String json2);
}
