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
package com.datastax.oss.streaming.ai.datasource;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface QueryStepDataSource extends AutoCloseable {

    default void initialize(Map<String, Object> config) throws Exception {}

    default List<Map<String, Object>> fetchData(String query, List<Object> params) {
        return Collections.emptyList();
    }

    default Map<String, Object> executeStatement(
            String query, List<String> generatedKeys, List<Object> params) {
        fetchData(query, params);
        return Map.of();
    }

    default void close() {}
}
