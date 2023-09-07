package ai.langstream.cli.api.model;

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
                                        (Map<String, Object>) map.get("authentication")))
                .collect(Collectors.toList());
    }

    @NoArgsConstructor
    @Data
    @AllArgsConstructor
    public static class Gateway {

        public static final String TYPE_PRODUCE = "produce";
        public static final String TYPE_CONSUME = "consume";

        String id;
        String type;
        List<String> parameters;
        Map<String, Object> authentication;
    }
}
