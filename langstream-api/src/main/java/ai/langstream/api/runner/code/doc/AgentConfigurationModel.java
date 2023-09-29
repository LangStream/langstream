package ai.langstream.api.runner.code.doc;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgentConfigurationModel {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentConfigurationProperty {
        private String description;
        boolean required;
        private String type;
        private Map<String, AgentConfigurationProperty> properties;
        private AgentConfigurationProperty items;
        private String defaultValue;
    }

    private String name;
    private String description;
    private Map<String, AgentConfigurationProperty> properties;
}
