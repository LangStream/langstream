package ai.langstream.deployer.k8s.api.crds.agents;

import ai.langstream.deployer.k8s.api.crds.NamespacedSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;

@Setter
@Getter
@ToString
@NoArgsConstructor
public class AgentSpec extends NamespacedSpec {

    public record Resources(int parallelism, int size) {}
    public record Disk(String agentId, long size, String type) {}
    public record Options(List<Disk> disks) {}
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private String agentId;
    private String applicationId;
    @Deprecated private String image;
    @Deprecated private String imagePullPolicy;
    private String agentConfigSecretRef;
    private String agentConfigSecretRefChecksum;
    private String codeArchiveId;
    private Resources resources;
    private String options;
    @JsonIgnore
    private Options parsedOptions;

    @SneakyThrows
    private Options parseOptions() {
        if (parsedOptions == null) {
            if (options != null) {
                parsedOptions = MAPPER.readValue(options, Options.class);
            }

        }
        return parsedOptions;
    }

    @SneakyThrows
    public void serializeAndSetOptions(Options options) {
        this.options = MAPPER.writeValueAsString(options);
    }

    @JsonIgnore
    public List<Disk> getDisks() {
        final Options options = parseOptions();
        if (options == null) {
            return List.of();
        }
        return options.disks();
    }
}
