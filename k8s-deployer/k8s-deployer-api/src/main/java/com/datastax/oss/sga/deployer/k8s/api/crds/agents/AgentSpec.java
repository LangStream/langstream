package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.deployer.k8s.api.crds.NamespacedSpec;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AgentSpec extends NamespacedSpec {
}
