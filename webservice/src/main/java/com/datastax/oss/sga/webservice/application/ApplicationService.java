package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.Topic;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.SneakyThrows;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
@Slf4j
public class ApplicationService {
    private final ApplicationDeployer deployer = ApplicationDeployer
            .builder()
            .registry(new ClusterRuntimeRegistry())// TODO: add config
            .pluginsRegistry(new PluginsRegistry())
            .build();

    private final GlobalMetadataService globalMetadataService;
    private final ApplicationStore applicationStore;

    private CodeStorage codeStorage;

    public ApplicationService(
            GlobalMetadataService globalMetadataService,
            ApplicationStore store) {
        this.globalMetadataService = globalMetadataService;
        this.applicationStore = store;
    }


    @SneakyThrows
    public Map<String, StoredApplication> getAllApplications(String tenant) {
        checkTenant(tenant);
        return applicationStore.list(tenant);
    }

    @SneakyThrows
    public void deployApplication(String tenant, String applicationId,
                                  ModelBuilder.ApplicationWithPackageInfo applicationInstance,
                                  String codeArchiveReference) {
        checkTenant(tenant);
        if (applicationStore.get(tenant, applicationId) != null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Application already exists");
        }
        validateExecutionPlan(applicationId, applicationInstance.getApplication());
        applicationStore.put(tenant, applicationId, applicationInstance.getApplication(), codeArchiveReference);
    }

    @SneakyThrows
    public void updateApplication(String tenant, String applicationId,
                                  ModelBuilder.ApplicationWithPackageInfo applicationInstance,
                                  String codeArchiveReference) {
        checkTenant(tenant);
        final Application mergedApplication = validateDeployAndMerge(tenant, applicationId, applicationInstance);
        applicationStore.put(tenant, applicationId, mergedApplication, codeArchiveReference);
    }


    private Application validateDeployAndMerge(String tenant, String applicationId,
                                               ModelBuilder.ApplicationWithPackageInfo applicationInstance) {


        final StoredApplication existing = applicationStore.get(tenant, applicationId);
        if (existing == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Application not found");
        }
        final Application newApplication = applicationInstance.getApplication();
        if (!applicationInstance.isHasInstanceDefinition() && !applicationInstance.isHasSecretDefinition()
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
        return newApplication;

    }

    private ExecutionPlan validateExecutionPlan(String applicationId, Application applicationInstance) {
        final ExecutionPlan newPlan =
                deployer.createImplementation(applicationId, applicationInstance);
        return newPlan;
    }

    private void validateUpdate(String tenant, StoredApplication existing, Secrets existingSecrets,
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
                throw new IllegalArgumentException("Detected a change in the agents which is not supported. "
                        + "Agent " + newAgent.getKey()
                        + " is not present in the existing application. Rename or adding new agents is not supported.");
            }

            final DefaultAgentNode existingDefaultAgent = (DefaultAgentNode) existingAgent;
            if (!Objects.equals(newDefaultAgent.getAgentType(), existingDefaultAgent.getAgentType())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(newDefaultAgent.getId(), "type",
                                existingDefaultAgent.getAgentType(), newDefaultAgent.getAgentType()));
            }

            if (!Objects.equals(newDefaultAgent.getComponentType(), existingDefaultAgent.getComponentType())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(newDefaultAgent.getId(), "type",
                                existingDefaultAgent.getComponentType(), newDefaultAgent.getComponentType()));
            }
            if (!Objects.equals(newDefaultAgent.getCustomMetadata(), existingDefaultAgent.getCustomMetadata())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(newDefaultAgent.getId(), "metadata",
                                existingDefaultAgent.getCustomMetadata(), newDefaultAgent.getCustomMetadata()));
            }
            if (!Objects.equals(newDefaultAgent.getInputConnection(), existingDefaultAgent.getInputConnection())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(newDefaultAgent.getId(), "input",
                                existingDefaultAgent.getInputConnection(), newDefaultAgent.getInputConnection()));
            }
            if (!Objects.equals(newDefaultAgent.getOutputConnection(), existingDefaultAgent.getOutputConnection())) {
                throw new IllegalArgumentException(
                        getAgentFieldUpdateInvalidErrorString(newDefaultAgent.getId(), "output",
                                existingDefaultAgent.getOutputConnection(), newDefaultAgent.getOutputConnection()));
            }
        }
    }


    void validateTopicsUpdate(ExecutionPlan existingPlan, ExecutionPlan newPlan) {
        final Map<TopicDefinition, Topic> existingTopics = existingPlan.getTopics();
        final Map<TopicDefinition, Topic> newTopics = newPlan.getTopics();
        if (existingTopics.size() != newTopics.size()) {
            throw new IllegalArgumentException("Detected a change in the topics which is not supported. "
                    + "New topics: " + newTopics.size() + ". Existing topics: " + existingTopics.size());
        }

        for (Map.Entry<TopicDefinition, Topic> newTopic : newTopics.entrySet()) {
            final String name = newTopic.getKey().getName();
            final TopicDefinition existingByName = existingTopics
                    .keySet()
                    .stream()
                    .filter(topicDefinition -> topicDefinition.getName().equals(name))
                    .findAny()
                    .orElse(null);

            if (existingByName == null) {
                throw new IllegalArgumentException("Detected a change in the topics which is not supported. "
                        + "Topic " + newTopic.getKey()
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
                + "New agents: " + newOne + ". Existing agents: " + existing;
    }

    private String getAgentFieldUpdateInvalidErrorString(String agentId, String field, Object existing, Object newOne) {
        return "Detected a change in the agents which is not supported. For agent %s field %s changed from %s to %s"
                .formatted(agentId, field, existing, newOne);
    }

    @SneakyThrows
    public StoredApplication getApplication(String tenant, String applicationId) {
        checkTenant(tenant);
        return applicationStore.get(tenant, applicationId);
    }

    @SneakyThrows
    public void deleteApplication(String tenant, String applicationId) {
        checkTenant(tenant);
        applicationStore.delete(tenant, applicationId);
    }

    private void checkTenant(String tenant) {
        if (globalMetadataService.getTenant(tenant) == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "tenant not found"
            );
        }
    }

    @SneakyThrows
    public List<ApplicationStore.PodLogHandler> getPodLogs(String tenant, String applicationId,
                                                           ApplicationStore.LogOptions logOptions) {
        checkTenant(tenant);
        return applicationStore.logs(tenant, applicationId, logOptions);
    }

}
