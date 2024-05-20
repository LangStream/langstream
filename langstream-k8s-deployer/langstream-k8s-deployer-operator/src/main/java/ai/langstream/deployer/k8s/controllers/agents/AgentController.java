/*
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
package ai.langstream.deployer.k8s.controllers.agents;

import ai.langstream.api.model.AgentLifecycleStatus;
import ai.langstream.deployer.k8s.PodTemplate;
import ai.langstream.deployer.k8s.ResolvedDeployerConfiguration;
import ai.langstream.deployer.k8s.agents.AgentResourceUnitConfiguration;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.agents.AgentStatus;
import ai.langstream.deployer.k8s.controllers.BaseController;
import ai.langstream.deployer.k8s.controllers.InfiniteRetry;
import ai.langstream.deployer.k8s.util.JSONComparator;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.deployer.k8s.util.SpecDiffer;
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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(
        namespaces = Constants.WATCH_ALL_NAMESPACES,
        name = "agent-controller",
        dependents = {
            @Dependent(type = AgentController.StsDependantResource.class),
            @Dependent(type = AgentController.ServiceDependantResource.class)
        },
        retry = InfiniteRetry.class)
@JBossLog
public class AgentController extends BaseController<AgentCustomResource>
        implements ErrorStatusHandler<AgentCustomResource> {

    @Override
    public ErrorStatusUpdateControl<AgentCustomResource> updateErrorStatus(
            AgentCustomResource agentCustomResource,
            Context<AgentCustomResource> context,
            Exception e) {
        agentCustomResource.getStatus().setStatus(AgentLifecycleStatus.error(e.getMessage()));
        return ErrorStatusUpdateControl.updateStatus(agentCustomResource);
    }

    @Override
    protected PatchResult patchResources(
            AgentCustomResource agent, Context<AgentCustomResource> context) {
        final String targetNamespace = agent.getMetadata().getNamespace();
        final String name =
                context.getSecondaryResource(StatefulSet.class)
                        .orElseThrow()
                        .getMetadata()
                        .getName();
        final StatefulSet current =
                client.apps().statefulSets().inNamespace(targetNamespace).withName(name).get();

        setLastAppliedConfig(agent);
        if (KubeUtil.isStatefulSetReady(current)) {
            agent.getStatus().setStatus(AgentLifecycleStatus.DEPLOYED);
            return PatchResult.patch(UpdateControl.updateStatus(agent));
        } else {
            agent.getStatus().setStatus(AgentLifecycleStatus.DEPLOYING);
            return PatchResult.patch(
                    UpdateControl.updateStatus(agent).rescheduleAfter(5, TimeUnit.SECONDS));
        }
    }

    private void setLastAppliedConfig(AgentCustomResource agent) {
        if (agent.getStatus().getLastConfigApplied() != null) {
            return;
        }
        final LastAppliedConfigForStatefulset lastAppliedConfigForStatefulset =
                new LastAppliedConfigForStatefulset(
                        configuration.getAgentResources(),
                        configuration.getRuntimeImage(),
                        configuration.getRuntimeImagePullPolicy(),
                        configuration.getAgentPodTemplate());

        agent.getStatus()
                .setLastConfigApplied(
                        SerializationUtil.writeAsJson(lastAppliedConfigForStatefulset));
    }

    @Override
    protected DeleteControl cleanupResources(
            AgentCustomResource resource, Context<AgentCustomResource> context) {
        return DeleteControl.defaultDelete();
    }

    @JBossLog
    public static class StsDependantResource
            extends CRUDKubernetesDependentResource<StatefulSet, AgentCustomResource> {

        @Inject ResolvedDeployerConfiguration configuration;

        public StsDependantResource() {
            super(StatefulSet.class);
        }

        @Override
        protected StatefulSet desired(
                AgentCustomResource primary, Context<AgentCustomResource> context) {
            try {
                final StatefulSet existingStatefulset =
                        context.getSecondaryResource(StatefulSet.class).orElse(null);
                final AgentResourcesFactory.GenerateStatefulsetParams
                                .GenerateStatefulsetParamsBuilder
                        builder =
                                AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                        .agentCustomResource(primary);

                final boolean isUpdate;

                final AgentStatus status = primary.getStatus();
                if (status != null && existingStatefulset != null) {
                    // spec has not changed, do not touch the statefulset at all
                    if (!areSpecChanged(primary)) {
                        log.infof(
                                "Agent %s spec has not changed, skipping statefulset update",
                                primary.getMetadata().getName());
                        return existingStatefulset;
                    }
                }

                if (status != null && status.getLastConfigApplied() != null) {
                    isUpdate = true;
                    // this is an update for the statefulset.
                    // It's required to not keep the same deployer configuration of the current
                    // version

                    boolean autoUpgradeRuntimeImage = primary.getSpec().isAutoUpgradeRuntimeImage();
                    boolean autoUpgradeRuntimeImagePullPolicy =
                            primary.getSpec().isAutoUpgradeRuntimeImagePullPolicy();
                    boolean autoUpgradeAgentResources =
                            primary.getSpec().isAutoUpgradeAgentResources();
                    boolean updatePodTemplate = primary.getSpec().isAutoUpgradeAgentPodTemplate();

                    final LastAppliedConfigForStatefulset lastAppliedConfig =
                            SerializationUtil.readJson(
                                    status.getLastConfigApplied(),
                                    LastAppliedConfigForStatefulset.class);
                    builder.agentResourceUnitConfiguration(
                                    autoUpgradeAgentResources
                                            ? configuration.getAgentResources()
                                            : lastAppliedConfig.getAgentResourceUnitConfiguration())
                            .image(
                                    autoUpgradeRuntimeImage
                                            ? configuration.getRuntimeImage()
                                            : lastAppliedConfig.getImage())
                            .imagePullPolicy(
                                    autoUpgradeRuntimeImagePullPolicy
                                            ? configuration.getRuntimeImagePullPolicy()
                                            : lastAppliedConfig.getImagePullPolicy())
                            .podTemplate(
                                    updatePodTemplate
                                            ? configuration.getAgentPodTemplate()
                                            : lastAppliedConfig.getPodTemplate());
                } else {
                    isUpdate = false;
                    builder.agentResourceUnitConfiguration(configuration.getAgentResources())
                            .podTemplate(configuration.getAgentPodTemplate())
                            .image(configuration.getRuntimeImage())
                            .imagePullPolicy(configuration.getRuntimeImagePullPolicy());
                }
                log.infof(
                        "Generating statefulset for agent %s (update=%s)",
                        primary.getMetadata().getName(), isUpdate + "");
                return AgentResourcesFactory.generateStatefulSet(builder.build());
            } catch (Throwable t) {
                log.errorf(
                        t,
                        "Error while generating StatefulSet for agent %s",
                        primary.getMetadata().getName());
                throw new RuntimeException(t);
            }
        }
    }

    @JBossLog
    public static class ServiceDependantResource
            extends CRUDKubernetesDependentResource<Service, AgentCustomResource> {

        @Inject ResolvedDeployerConfiguration configuration;

        public ServiceDependantResource() {
            super(Service.class);
        }

        @Override
        protected Service desired(
                AgentCustomResource primary, Context<AgentCustomResource> context) {
            try {
                return AgentResourcesFactory.generateHeadlessService(primary);
            } catch (Throwable t) {
                log.errorf(
                        t,
                        "Error while generating Service for agent %s",
                        primary.getMetadata().getName());
                throw new RuntimeException(t);
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LastAppliedConfigForStatefulset {

        private AgentResourceUnitConfiguration agentResourceUnitConfiguration;
        private String image;
        private String imagePullPolicy;
        private PodTemplate podTemplate;
    }

    protected static boolean areSpecChanged(AgentCustomResource cr) {
        final String lastApplied = cr.getStatus().getLastApplied();
        if (lastApplied == null) {
            return true;
        }
        final JSONComparator.Result diff = SpecDiffer.generateDiff(lastApplied, cr.getSpec());
        if (!diff.areEquals()) {
            log.infof("Spec changed for %s", cr.getMetadata().getName());
            SpecDiffer.logDetailedSpecDiff(diff);
            return true;
        }
        return false;
    }
}
