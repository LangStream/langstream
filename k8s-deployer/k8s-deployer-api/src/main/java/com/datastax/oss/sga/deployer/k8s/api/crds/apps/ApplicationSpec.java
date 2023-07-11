package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@SuperBuilder
public class ApplicationSpec extends NamespacedSpec {
    private String image;
    private String imagePullPolicy;
    private Map<String, Resource> resources = new HashMap<>();
    private Map<String, Module> modules = new HashMap<>();
    private Instance instance;

}
