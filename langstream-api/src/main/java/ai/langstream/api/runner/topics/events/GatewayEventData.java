package ai.langstream.api.runner.topics.events;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GatewayEventData {

    private Map<String, String> userParameters;
    private Map<String, String> options;
    private Map<String, String> httpRequestHeaders;

}
