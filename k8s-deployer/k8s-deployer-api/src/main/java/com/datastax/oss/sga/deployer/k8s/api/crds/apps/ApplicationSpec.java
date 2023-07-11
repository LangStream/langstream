package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApplicationSpec extends NamespacedSpec {
    private String image;
    private String imagePullPolicy;
    private String application;

}
