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
package ai.langstream.cli.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class Gateways {

    private static final ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    public static List<Gateway> readFromApplicationDescription(String content) {
        Map<String, Object> applicationDescription = mapper.readValue(content, Map.class);
        final Map<String, Object> application =
                (Map<String, Object>) applicationDescription.get("application");
        final Map<String, Object> gateways = (Map<String, Object>) application.get("gateways");
        if (gateways == null) {
            return List.of();
        }
        final List<Map<String, Object>> gatewayList =
                (List<Map<String, Object>>) gateways.get("gateways");
        if (gatewayList == null) {
            return List.of();
        }
        return gatewayList.stream()
                .map(
                        map ->
                                new Gateway(
                                        (String) map.get("id"),
                                        (String) map.get("type"),
                                        (List<String>) map.get("parameters"),
                                        (Map<String, Object>) map.get("authentication"),
                                        (Map<String, Object>) map.get("chat-options")))
                .collect(Collectors.toList());
    }

    @NoArgsConstructor
    @Data
    @AllArgsConstructor
    public static class Gateway {

        public static final String TYPE_PRODUCE = "produce";
        public static final String TYPE_CONSUME = "consume";
        public static final String TYPE_CHAT = "chat";

        String id;
        String type;
        List<String> parameters;
        Map<String, Object> authentication;

        @JsonProperty("chat-options")
        Map<String, Object> chatOptions;
    }
}
