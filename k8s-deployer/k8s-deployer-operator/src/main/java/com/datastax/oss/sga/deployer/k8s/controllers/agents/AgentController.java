package com.datastax.oss.sga.deployer.k8s.controllers.agents;

import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourceUnitConfiguration;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.controllers.BaseController;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import java.util.Map;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(
        namespaces = Constants.WATCH_ALL_NAMESPACES,
        name = "agent-controller",
        dependents = {
                @Dependent(type = AgentController.StsDependantResource.class)
        })
@JBossLog
public class AgentController extends BaseController<AgentCustomResource>
        implements ErrorStatusHandler<AgentCustomResource> {

    @Override
    public ErrorStatusUpdateControl<AgentCustomResource> updateErrorStatus(AgentCustomResource agentCustomResource,
                                                                           Context<AgentCustomResource> context,
                                                                           Exception e) {
        agentCustomResource.getStatus().setStatus(AgentLifecycleStatus.error(e.getMessage()));
        return ErrorStatusUpdateControl.updateStatus(agentCustomResource);
    }

    @Override
    protected UpdateControl<AgentCustomResource> patchResources(AgentCustomResource agent,
                                                                Context<AgentCustomResource> context) {
        final String targetNamespace = agent.getMetadata().getNamespace();
        final String name = context.getSecondaryResource(StatefulSet.class).orElseThrow()
                .getMetadata().getName();
        final StatefulSet current = client.apps().statefulSets()
                .inNamespace(targetNamespace)
                .withName(name)
                .get();
        if (KubeUtil.isStatefulSetReady(current)) {
            agent.getStatus().setStatus(AgentLifecycleStatus.DEPLOYED);
            return UpdateControl.updateStatus(agent);
        } else {
            agent.getStatus().setStatus(AgentLifecycleStatus.DEPLOYING);
            return UpdateControl.updateStatus(agent)
                    .rescheduleAfter(5, TimeUnit.SECONDS);
        }
    }

    @Override
    protected DeleteControl cleanupResources(AgentCustomResource resource,
                                             Context<AgentCustomResource> context) {
        return DeleteControl.defaultDelete();
    }


    public static class StsDependantResource extends
            CRUDKubernetesDependentResource<StatefulSet, AgentCustomResource> {

        @Inject
        protected DeployerConfiguration configuration;

        public StsDependantResource() {
            super(StatefulSet.class);
        }

        @Override
        protected StatefulSet desired(AgentCustomResource primary, Context<AgentCustomResource> context) {
            final Map<String, Object> codeStorage = SerializationUtil.readYaml(configuration.codeStorage(), Map.class);
            final AgentResourceUnitConfiguration agentResourceUnitConfiguration = SerializationUtil.readYaml(
                    configuration.agentResources(), AgentResourceUnitConfiguration.class);
            return AgentResourcesFactory.generateStatefulSet(primary, codeStorage, agentResourceUnitConfiguration);
        }
    }

}
