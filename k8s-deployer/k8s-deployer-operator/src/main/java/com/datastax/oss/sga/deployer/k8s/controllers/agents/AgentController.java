/**
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sga.deployer.k8s.controllers.agents;

import ai.langstream.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.ResolvedDeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.controllers.BaseController;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import io.fabric8.kubernetes.api.model.Service;
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
import jakarta.inject.Inject;

import java.util.concurrent.TimeUnit;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(
        namespaces = Constants.WATCH_ALL_NAMESPACES,
        name = "agent-controller",
        dependents = {
                @Dependent(type = AgentController.StsDependantResource.class),
                @Dependent(type = AgentController.ServiceDependantResource.class)
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


    @JBossLog
    public static class StsDependantResource extends
            CRUDKubernetesDependentResource<StatefulSet, AgentCustomResource> {

        @Inject
        ResolvedDeployerConfiguration configuration;

        public StsDependantResource() {
            super(StatefulSet.class);
        }

        @Override
        protected StatefulSet desired(AgentCustomResource primary, Context<AgentCustomResource> context) {
            try {
                return AgentResourcesFactory.generateStatefulSet(primary, configuration.getCodeStorage(),
                        configuration.getAgentResources(), configuration.getPodTemplate());
            } catch (Throwable t) {
                log.errorf(t, "Error while generating StatefulSet for agent %s", primary.getMetadata().getName());
                throw new RuntimeException(t);
            }
        }
    }

    @JBossLog
    public static class ServiceDependantResource extends
            CRUDKubernetesDependentResource<Service, AgentCustomResource> {

        @Inject
        ResolvedDeployerConfiguration configuration;

        public ServiceDependantResource() {
            super(Service.class);
        }

        @Override
        protected Service desired(AgentCustomResource primary, Context<AgentCustomResource> context) {
            try {
                return AgentResourcesFactory.generateHeadlessService(primary);
            } catch (Throwable t) {
                log.errorf(t, "Error while generating Service for agent %s", primary.getMetadata().getName());
                throw new RuntimeException(t);
            }
        }
    }

}
