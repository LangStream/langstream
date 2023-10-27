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
package ai.langstream.apigateway.util;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HttpUtil {

    public static Map<String, String> parseQuerystring(String queryString) {
        Map<String, String> map = new HashMap<>();
        if (queryString == null || queryString.isBlank()) {
            return map;
        }
        String[] params = queryString.split("&");
        for (String param : params) {
            String[] keyValuePair = param.split("=", 2);
            String name = URLDecoder.decode(keyValuePair[0], StandardCharsets.UTF_8);
            if ("".equals(name)) {
                continue;
            }
            String value =
                    keyValuePair.length > 1
                            ? URLDecoder.decode(keyValuePair[1], StandardCharsets.UTF_8)
                            : "";
            map.put(name, value);
        }
        return map;
    }
}
