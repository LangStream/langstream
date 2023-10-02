package ai.langstream.api.doc;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConfigPropertyModel {
    private String description;
    boolean required;
    private String type;
    private Map<String, ConfigPropertyModel> properties;
    private ConfigPropertyModel items;
    private Object defaultValue;
}
