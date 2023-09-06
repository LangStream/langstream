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
public class EventRecord {

    public enum Categories {
        Gateway
    }

    public enum Types {
        // gateway
        ClientConnected,
        ClientDisconnected;
    }

    private Categories category;
    private String type;
    private long timestamp;
    private Map<String, Object> source;
    private Map<String, Object> data;

}
