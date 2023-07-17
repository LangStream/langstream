package com.datastax.oss.sga.deployer.k8s.api.crds.agents;

import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
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
public class AgentCustomResource extends CustomResource<AgentSpec, AgentStatus> implements Namespaced {

    @Override
    protected AgentStatus initStatus() {
        final AgentStatus agentStatus = new AgentStatus();
        agentStatus.setStatus(AgentLifecycleStatus.CREATED);
        return agentStatus;
    }

}
