package com.datastax.oss.sga.deployer.k8s;

import com.datastax.oss.sga.deployer.k8s.agents.AgentResourceUnitConfiguration;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import lombok.Getter;

@ApplicationScoped
public class ResolvedDeployerConfiguration {

    public ResolvedDeployerConfiguration(DeployerConfiguration configuration) {
        this.clusterRuntime = SerializationUtil.readYaml(configuration.clusterRuntime(), Map.class);
        this.codeStorage = SerializationUtil.readYaml(configuration.codeStorage(), Map.class);
        this.agentResources = SerializationUtil.readYaml(configuration.agentResources(), AgentResourceUnitConfiguration.class);
    }

    @Getter
    private Map<String, Object> clusterRuntime;

    @Getter
    private Map<String, Object> codeStorage;

    @Getter
    private AgentResourceUnitConfiguration agentResources;
}
