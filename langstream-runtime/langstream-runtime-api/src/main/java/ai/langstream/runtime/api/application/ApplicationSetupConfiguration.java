package ai.langstream.runtime.api.application;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationSetupConfiguration {
    private String applicationId;
    private String tenant;
    private String application;
}
