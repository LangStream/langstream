package ai.langstream.deployer.k8s;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GlobalStorageConfiguration {

    private String type;
    private Map<String, Object> configuration = new HashMap<>();
}
