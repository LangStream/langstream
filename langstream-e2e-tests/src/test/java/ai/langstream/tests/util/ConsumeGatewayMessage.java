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
package ai.langstream.tests.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumeGatewayMessage {
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @SneakyThrows
    public static ConsumeGatewayMessage readValue(String line) {
        return JSON_MAPPER.readValue(line, ConsumeGatewayMessage.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Record {
        private Object key;
        private Object value;
        private Map<String, String> headers;
    }

    private Record record;
    private String offset;

    @SneakyThrows
    public String getAnswerFromChatCompletionsValue() {
        final Map<String, Object> chatHistoryModel =
                JSON_MAPPER.readValue((String) record.getValue(), Map.class);
        final String answer = chatHistoryModel.get("answer").toString();
        return answer;
    }

    @SneakyThrows
    public Map<String, Object> recordValueAsMap() {
        return JSON_MAPPER.readValue((String) record.getValue(), Map.class);
    }
}
