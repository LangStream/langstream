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
package ai.langstream.webservice.application;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.*;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.impl.storage.GlobalMetadataStoreManager;
import ai.langstream.webservice.common.GlobalMetadataService;
import ai.langstream.webservice.config.ApplicationDeployProperties;
import ai.langstream.webservice.config.TenantProperties;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
@Slf4j
@AllArgsConstructor
public class ApplicationService {
    private final ApplicationDeployer deployer =
            ApplicationDeployer.builder()
                    .registry(new ClusterRuntimeRegistry())
                    .pluginsRegistry(new PluginsRegistry())
                    .topicConnectionsRuntimeRegistry(new TopicConnectionsRuntimeRegistry())
                    .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                    .build();

    private final GlobalMetadataService globalMetadataService;
    private final ApplicationStore applicationStore;
    private final ApplicationDeployProperties applicationDeployProperties;
    private final TenantProperties tenantProperties;

    @SneakyThrows
    public Map<String, StoredApplication> getAllApplications(String tenant) {
        checkTenant(tenant);
        return applicationStore.list(tenant);
    }

    @SneakyThrows
    public void deployApplication(
            String tenant,
            String applicationId,
            ModelBuilder.ApplicationWithPackageInfo applicationInstance,
            String codeArchiveReference,
            boolean autoUpgrade) {
        checkTenant(tenant);
        if (applicationStore.get(tenant, applicationId, false) != null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Application already exists");
        }

        validateApplicationModel(applicationInstance.getApplication());
        ExecutionPlan executionPlan =
                validateExecutionPlan(applicationId, applicationInstance.getApplication());

        checkResourceUsage(tenant, applicationId, executionPlan);

        applicationStore.put(
                tenant,
                applicationId,
                applicationInstance.getApplication(),
                codeArchiveReference,
                executionPlan,
                autoUpgrade,
                false);
    }

    void checkResourceUsage(String tenant, String applicationId, ExecutionPlan executionPlan) {
        final TenantConfiguration config = globalMetadataService.getTenant(tenant);
        int maxTotalResourceUnits = config.getMaxTotalResourceUnits();
        if (maxTotalResourceUnits <= 0) {
            maxTotalResourceUnits = tenantProperties.getDefaultMaxTotalResourceUnits();
        }
        if (maxTotalResourceUnits <= 0) {
            return;
        }

        final int requestedUnits = countRequestedUnits(executionPlan);
        final Map<String, Integer> appUsage = applicationStore.getResourceUsage(tenant);
        final Integer currentUsage =
                appUsage.entrySet().stream()
                        .filter(e -> !e.getKey().equals(applicationId))
                        .collect(Collectors.summingInt(e -> e.getValue()));
        final int totalUsage = currentUsage + requestedUnits;
        if (maxTotalResourceUnits < totalUsage) {
            throw new IllegalArgumentException(
                    "Not enough resources to deploy application " + applicationId);
        }
    }

    private int countRequestedUnits(ExecutionPlan executionPlan) {
        int requestedUnits = 0;

        for (Map.Entry<String, AgentNode> agent : executionPlan.getAgents().entrySet()) {
            final ResourcesSpec resources = agent.getValue().getResources();
            requestedUnits += resources.size() * resources.parallelism();
        }
        return requestedUnits;
    }

    private void validateApplicationModel(Application application) {
        validateGateways(application);
    }

    private void validateGateways(Application application) {
        if (applicationDeployProperties.gateway().requireAuthentication()) {
            if (application.getGateways() != null && application.getGateways().gateways() != null) {
                for (Gateway gateway : application.getGateways().gateways()) {
                    if (gateway.getAuthentication() == null) {
                        throw new IllegalArgumentException(
                                "Gateway " + gateway.getId() + " is missing authentication");
                    }
                }
            }
        }
    }

    @SneakyThrows
    public void updateApplication(
            String tenant,
            String applicationId,
            ModelBuilder.ApplicationWithPackageInfo applicationInstance,
            String codeArchiveReference,
            boolean autoUpgrade,
            boolean forceRestart) {
        checkTenant(tenant);
        validateDeployMergeAndUpdate(
                tenant,
                applicationId,
                applicationInstance,
                codeArchiveReference,
                autoUpgrade,
                forceRestart);
    }

    private void validateDeployMergeAndUpdate(
            String tenant,
            String applicationId,
            ModelBuilder.ApplicationWithPackageInfo applicationInstance,
            String codeArchiveReference,
            boolean autoUpgrade,
            boolean forceRestart) {

        final StoredApplication existing = applicationStore.get(tenant, applicationId, false);
        if (existing == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Application not found");
        }
        final Application newApplication = applicationInstance.getApplication();
        if (!applicationInstance.isHasInstanceDefinition()
                && !applicationInstance.isHasSecretDefinition()
                && !applicationInstance.isHasAppDefinition()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No changes detected");
        }
        if (!applicationInstance.isHasInstanceDefinition()) {
            log.info("Apply existing instance to application {}", applicationId);
            newApplication.setInstance(existing.getInstance().getInstance());
        }
        final Secrets existingSecrets = applicationStore.getSecrets(tenant, applicationId);
        if (!applicationInstance.isHasSecretDefinition()) {
            if (existingSecrets != null) {
                log.info("Apply existing secrets to application {}", applicationId);
                newApplication.setSecrets(existingSecrets);
            }
        }
        if (!applicationInstance.isHasAppDefinition()) {
            log.info("Apply existing pipelines and configuration to application {}", applicationId);
            newApplication.setDependencies(existing.getInstance().getDependencies());
            newApplication.setModules(existing.getInstance().getModules());
            newApplication.setResources(existing.getInstance().getResources());
        }
        final ExecutionPlan newPlan = validateExecutionPlan(applicationId, newApplication);
        validateUpdate(tenant, existing, existingSecrets, newPlan);
        if (codeArchiveReference == null) {
            codeArchiveReference = existing.getCodeArchiveReference();
        }
        applicationStore.put(
                tenant,
                applicationId,
                newApplication,
                codeArchiveReference,
                newPlan,
                autoUpgrade,
                forceRestart);
    }

    ExecutionPlan validateExecutionPlan(String applicationId, Application applicationInstance) {
        return deployer.createImplementation(applicationId, applicationInstance);
    }

    private void validateUpdate(
            String tenant,
            StoredApplication existing,
            Secrets existingSecrets,
            ExecutionPlan newPlan) {
        existing.getInstance().setSecrets(existingSecrets);
        final ExecutionPlan existingPlan =
                deployer.createImplementation(existing.getApplicationId(), existing.getInstance());
        validateTopicsUpdate(existingPlan, newPlan);
        validateAgentsUpdate(existingPlan, newPlan);
    }

    private void validateAgentsUpdate(ExecutionPlan existingPlan, ExecutionPlan newPlan) {
        final Map<String, AgentNode> existingAgents = existingPlan.getAgents();
        final Map<String, AgentNode> newAgents = newPlan.getAgents();
        if (existingAgents.size() != newAgents.size()) {
            throw new IllegalArgumentException(
                    getAgentUpdateInvalidErrorString(existingAgents.size(), newAgents.size()));
        }
        for (Map.Entry<String, AgentNode> newAgent : newAgents.entrySet()) {
            final AgentNode existingAgent = existingAgents.get(newAgent.getKey());
            final DefaultAgentNode newDefaultAgent = (DefaultAgentNode) newAgent.getValue();
            if (existingAgent == null) {
                throw new IllegalArgumentException(
                        "Detected a change in the agents which is not supported. "
                                + "Agent "
                                + newAgent.getKey()
                                + " is not present in the existing application. Rename or adding new agents is not supported.");
            }

            final DefaultAgentNode existingDefaultAgent = (DefaultAgentNode) existingAgent;
            if (!Objects.equals(
                    newDefaultAgent.getAgentType(), existingDefaultAgent.getAgentType())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(
                                newDefaultAgent.getId(),
                                "type",
                                existingDefaultAgent.getAgentType(),
                                newDefaultAgent.getAgentType()));
            }

            if (!Objects.equals(
                    newDefaultAgent.getComponentType(), existingDefaultAgent.getComponentType())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(
                                newDefaultAgent.getId(),
                                "type",
                                existingDefaultAgent.getComponentType(),
                                newDefaultAgent.getComponentType()));
            }
            if (!Objects.equals(
                    newDefaultAgent.getCustomMetadata(),
                    existingDefaultAgent.getCustomMetadata())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(
                                newDefaultAgent.getId(),
                                "metadata",
                                existingDefaultAgent.getCustomMetadata(),
                                newDefaultAgent.getCustomMetadata()));
            }
            if (!Objects.equals(
                    newDefaultAgent.getInputConnectionImplementation(),
                    existingDefaultAgent.getInputConnectionImplementation())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(
                                newDefaultAgent.getId(),
                                "input",
                                existingDefaultAgent.getInputConnectionImplementation(),
                                newDefaultAgent.getInputConnectionImplementation()));
            }
            if (!Objects.equals(
                    newDefaultAgent.getOutputConnectionImplementation(),
                    existingDefaultAgent.getOutputConnectionImplementation())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(
                                newDefaultAgent.getId(),
                                "output",
                                existingDefaultAgent.getOutputConnectionImplementation(),
                                newDefaultAgent.getOutputConnectionImplementation()));
            }
        }
    }

    void validateTopicsUpdate(ExecutionPlan existingPlan, ExecutionPlan newPlan) {
        final Map<TopicDefinition, Topic> existingTopics = existingPlan.getTopics();
        final Map<TopicDefinition, Topic> newTopics = newPlan.getTopics();
        if (existingTopics.size() != newTopics.size()) {
            throw new IllegalArgumentException(
                    "Detected a change in the topics which is not supported. "
                            + "New topics: "
                            + newTopics.size()
                            + ". Existing topics: "
                            + existingTopics.size());
        }

        for (Map.Entry<TopicDefinition, Topic> newTopic : newTopics.entrySet()) {
            final String name = newTopic.getKey().getName();
            final TopicDefinition existingByName =
                    existingTopics.keySet().stream()
                            .filter(topicDefinition -> topicDefinition.getName().equals(name))
                            .findAny()
                            .orElse(null);

            if (existingByName == null) {
                throw new IllegalArgumentException(
                        "Detected a change in the topics which is not supported. "
                                + "Topic "
                                + newTopic.getKey()
                                + " is not present in the existing application. Rename or adding new topics is not supported.");
            }
            if (!Objects.equals(newTopic.getKey(), existingByName)) {
                throw new IllegalArgumentException(
                        "Detected a change in the topics which is not supported. Topic %s has changed from: %s to: %s"
                                .formatted(name, existingByName, newTopic.getKey()));
            }
        }
    }

    private String getAgentUpdateInvalidErrorString(Object existing, Object newOne) {
        return "Detected a change in the agents which is not supported. "
                + "New agents: "
                + newOne
                + ". Existing agents: "
                + existing;
    }

    private String getAgentFieldUpdateInvalidErrorString(
            String agentId, String field, Object existing, Object newOne) {
        return "Detected a change in the agents which is not supported. For agent %s field %s changed from %s to %s"
                .formatted(agentId, field, existing, newOne);
    }

    @SneakyThrows
    public StoredApplication getApplication(
            String tenant, String applicationId, boolean queryPods) {
        checkTenant(tenant);
        return applicationStore.get(tenant, applicationId, queryPods);
    }

    @SneakyThrows
    public ApplicationSpecs getApplicationSpecs(String tenant, String applicationId) {
        checkTenant(tenant);
        return applicationStore.getSpecs(tenant, applicationId);
    }

    @SneakyThrows
    public void deleteApplication(String tenant, String applicationId, boolean force) {
        checkTenant(tenant);
        applicationStore.delete(tenant, applicationId, force);
    }

    private void checkTenant(String tenant) {
        try {
            globalMetadataService.validateTenant(tenant, true);
        } catch (GlobalMetadataStoreManager.TenantNotFoundException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        }
    }

    @SneakyThrows
    public List<ApplicationStore.PodLogHandler> getPodLogs(
            String tenant, String applicationId, ApplicationStore.LogOptions logOptions) {
        checkTenant(tenant);
        return applicationStore.logs(tenant, applicationId, logOptions);
    }
}
