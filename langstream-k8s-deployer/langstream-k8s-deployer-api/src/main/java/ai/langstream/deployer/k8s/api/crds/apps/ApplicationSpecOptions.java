package ai.langstream.deployer.k8s.api.crds.apps;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ApplicationSpecOptions {

    public enum DeleteMode {
        CLEANUP_REQUIRED,
        CLEANUP_BEST_EFFORT;
    }


    private DeleteMode deleteMode = DeleteMode.CLEANUP_REQUIRED;
    private boolean markedForDeletion;

}
