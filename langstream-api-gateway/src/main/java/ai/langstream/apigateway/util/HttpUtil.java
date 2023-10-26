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
