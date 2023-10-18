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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InterpolationUtils {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    public static String interpolate(String query, List<Object> array) {
        if (query == null || !query.contains("?")) {
            return query;
        }
        for (Object value : array) {
            int questionMark = query.indexOf("?");
            if (questionMark < 0) {
                return query;
            }
            String valueAsString = convertValueToJson(value);
            query =
                    query.substring(0, questionMark)
                            + valueAsString
                            + query.substring(questionMark + 1);
        }

        return query;
    }

    @SneakyThrows
    private static String convertValueToJson(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    public static final <R> R buildObjectFromJson(
            String json, Class<R> jsonModel, List<Object> params) {
        R parsedQuery;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Query {}", json);
                params.forEach(
                        param ->
                                log.debug(
                                        "Param {} {}",
                                        param,
                                        param != null ? param.getClass() : null));
            }

            // interpolate the query
            json = interpolate(json, params);
            if (log.isDebugEnabled()) {
                log.debug("Interpolated query {}", json);
            }
            parsedQuery = MAPPER.readValue(json, jsonModel);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Parsed query: {}", parsedQuery);
        }
        return parsedQuery;
    }

    public static final <R> R buildObjectFromJson(
            String json, Class<R> jsonModel, List<Object> params, ObjectMapper mapper) {
        R parsedQuery;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Query {}", json);
                params.forEach(
                        param ->
                                log.debug(
                                        "Param {} {}",
                                        param,
                                        param != null ? param.getClass() : null));
            }
            // interpolate the query
            json = interpolate(json, params);
            if (log.isDebugEnabled()) {
                log.debug("Interpolated query {}", json);
            }
            parsedQuery = mapper.readValue(json, jsonModel);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Parsed query: {}", parsedQuery);
        }
        return parsedQuery;
    }
}
