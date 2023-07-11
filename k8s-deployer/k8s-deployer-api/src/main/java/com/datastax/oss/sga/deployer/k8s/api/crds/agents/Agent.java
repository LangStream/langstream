package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("sga.oss.datastax.com")
@Kind("Agent")
@Singular("agent")
@Plural("agents")
public class Agent extends CustomResource<AgentSpec, AgentStatus> implements Namespaced {

    @Override
    protected AgentStatus initStatus() {
        return new AgentStatus();
    }

}
