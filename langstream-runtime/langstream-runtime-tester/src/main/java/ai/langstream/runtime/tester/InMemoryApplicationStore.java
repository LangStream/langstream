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
package ai.langstream.runtime.tester;

import ai.langstream.api.model.AgentLifecycleStatus;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.ApplicationStatus;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.runner.code.AgentStatusResponse;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.apps.SerializedApplicationInstance;
import ai.langstream.runtime.agent.api.AgentAPIController;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class InMemoryApplicationStore implements ApplicationStore {

    private static final ConcurrentHashMap<String, LocalApplication> APPLICATIONS =
            new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Secrets> SECRETS = new ConcurrentHashMap<>();

    public interface AgentInfoCollector {
        public Map<String, AgentAPIController> collectAgentsStatus();
    }

    private static AgentInfoCollector agentsInfoCollector = Map::of;
    private static Collection<String> filterAgents = null;

    public static void setAgentsInfoCollector(AgentInfoCollector agentsInfoCollector) {
        InMemoryApplicationStore.agentsInfoCollector = agentsInfoCollector;
    }

    public static void setFilterAgents(Collection<String> filterAgents) {
        InMemoryApplicationStore.filterAgents = filterAgents;
    }

    @Override
    public void validateTenant(String tenant, boolean failIfNotExists)
            throws IllegalStateException {}

    @Override
    public void onTenantCreated(String tenant) {}

    @Override
    public void onTenantUpdated(String tenant) {}

    @Override
    public void onTenantDeleted(String tenant) {}

    record LocalApplication(
            String tenant,
            String applicationId,
            Application applicationInstance,
            String codeArchiveReference,
            ExecutionPlan executionPlan) {}

    @Override
    public void put(
            String tenant,
            String applicationId,
            Application applicationInstance,
            String codeArchiveReference,
            ExecutionPlan executionPlan,
            boolean autoUpgrade,
            boolean forceRestart) {
        APPLICATIONS.put(
                getKey(tenant, applicationId),
                new LocalApplication(
                        tenant,
                        applicationId,
                        applicationInstance,
                        codeArchiveReference,
                        executionPlan));
        Secrets secrets = applicationInstance.getSecrets();
        if (secrets == null) {
            secrets = new Secrets(Map.of());
        }
        SECRETS.put(getKey(tenant, applicationId), secrets);
    }

    @Override
    public StoredApplication get(String tenant, String applicationId, boolean queryPods) {
        LocalApplication localApplication = APPLICATIONS.get(getKey(tenant, applicationId));
        if (localApplication == null) {
            return null;
        }
        return buildMockStoredApplication(localApplication, queryPods);
    }

    @Override
    public ApplicationSpecs getSpecs(String tenant, String applicationId) {
        LocalApplication localApplication = APPLICATIONS.get(getKey(tenant, applicationId));
        if (localApplication == null) {
            return null;
        }
        return new ApplicationSpecs(
                applicationId,
                localApplication.applicationInstance(),
                localApplication.codeArchiveReference());
    }

    @Override
    public Secrets getSecrets(String tenant, String applicationId) {
        return SECRETS.get(getKey(tenant, applicationId));
    }

    @NotNull
    private static String getKey(String tenant, String applicationId) {
        return tenant + "-" + applicationId;
    }

    @Override
    public void delete(String tenant, String applicationId, boolean force) {
        APPLICATIONS.remove(getKey(tenant, applicationId));
        SECRETS.remove(getKey(tenant, applicationId));
    }

    @Override
    public Map<String, StoredApplication> list(String tenant) {
        return APPLICATIONS.values().stream()
                .filter(a -> tenant.equals(a.tenant))
                .collect(
                        Collectors.toMap(
                                LocalApplication::applicationId,
                                a ->
                                        InMemoryApplicationStore.buildMockStoredApplication(
                                                a, false)));
    }

    @NotNull
    private static StoredApplication buildMockStoredApplication(
            LocalApplication a, boolean queryPods) {
        return new StoredApplication(
                a.applicationId,
                a.applicationInstance,
                a.codeArchiveReference,
                buildMockApplicationStatus(a, queryPods));
    }

    @NotNull
    private static ApplicationStatus buildMockApplicationStatus(
            LocalApplication localApplication, boolean queryPods) {

        SerializedApplicationInstance serializedApplicationInstance =
                new SerializedApplicationInstance(
                        localApplication.applicationInstance, localApplication.executionPlan);
        Map<String, SerializedApplicationInstance.AgentRunnerDefinition> agentRunners =
                serializedApplicationInstance.getAgentRunners();
        List<String> declaredAgents = new ArrayList<>();

        Map<String, AgentResourcesFactory.AgentRunnerSpec> agentRunnerSpecMap = new HashMap<>();
        agentRunners.forEach(
                (agentId, agentRunnerDefinition) -> {
                    log.debug("Adding agent id {} (def {})", agentId, agentRunnerDefinition);

                    if (filterAgents != null
                            && !filterAgents.contains(agentRunnerDefinition.getAgentId())) {
                        log.info(
                                "Ignoring agent id {} (filtered out, keep only {})",
                                agentRunnerDefinition.getAgentId(),
                                filterAgents);
                        return;
                    }

                    // this agentId doesn't contain the "module" prefix
                    declaredAgents.add(agentRunnerDefinition.getAgentId());
                    agentRunnerSpecMap.put(
                            agentId,
                            AgentResourcesFactory.AgentRunnerSpec.builder()
                                    .agentId(agentRunnerDefinition.getAgentId())
                                    .agentType(agentRunnerDefinition.getAgentType())
                                    .componentType(agentRunnerDefinition.getComponentType())
                                    .configuration(agentRunnerDefinition.getConfiguration())
                                    .inputTopic(agentRunnerDefinition.getInputTopic())
                                    .outputTopic(agentRunnerDefinition.getOutputTopic())
                                    .build());
                });

        Map<String, ApplicationStatus.AgentStatus> agents = new HashMap<>();

        Map<String, AgentAPIController> agentsInfo = agentsInfoCollector.collectAgentsStatus();

        for (String declaredAgent : declaredAgents) {
            ApplicationStatus.AgentStatus agentStatus = new ApplicationStatus.AgentStatus();
            agentStatus.setStatus(AgentLifecycleStatus.DEPLOYED);
            AgentResourcesFactory.AgentRunnerSpec agentRunnerSpec =
                    agentRunnerSpecMap.values().stream()
                            .filter(a -> a.getAgentId().equals(declaredAgent))
                            .findFirst()
                            .orElse(null);

            if (agentRunnerSpec == null) {
                throw new IllegalStateException("No agent runner spec for agent " + declaredAgent);
            }

            AgentAPIController agentAPIController = agentsInfo.get(declaredAgent);
            Map<String, ApplicationStatus.AgentWorkerStatus> podStatuses =
                    getPodStatuses(agentAPIController, agentRunnerSpec, queryPods);
            agentStatus.setWorkers(podStatuses);

            agents.put(declaredAgent, agentStatus);
        }

        ApplicationStatus result = new ApplicationStatus();
        result.setStatus(ApplicationLifecycleStatus.DEPLOYED);
        result.setAgents(agents);
        return result;
    }

    private static Map<String, ApplicationStatus.AgentWorkerStatus> getPodStatuses(
            AgentAPIController agentAPIController,
            final AgentResourcesFactory.AgentRunnerSpec agentRunnerSpec,
            boolean queryPods) {

        Map<String, ApplicationStatus.AgentWorkerStatus> result = new HashMap<>();
        ApplicationStatus.AgentWorkerStatus agentWorkerStatus =
                new ApplicationStatus.AgentWorkerStatus();
        agentWorkerStatus.setStatus(ApplicationStatus.AgentWorkerStatus.Status.RUNNING);

        // definition from the execution plan

        agentWorkerStatus =
                agentWorkerStatus.withAgentSpec(
                        agentRunnerSpec.getAgentId(),
                        agentRunnerSpec.getAgentType(),
                        agentRunnerSpec.getComponentType(),
                        agentRunnerSpec.getConfiguration(),
                        agentRunnerSpec.getInputTopic(),
                        agentRunnerSpec.getOutputTopic());

        if (queryPods) {
            // status of the agents
            List<AgentStatusResponse> agentStatusResponses = agentAPIController.serveWorkerStatus();
            agentWorkerStatus = agentWorkerStatus.applyAgentStatus(agentStatusResponses);
        }

        result.put("docker", agentWorkerStatus);
        return result;
    }

    @Override
    public Map<String, Integer> getResourceUsage(String tenant) {
        notSupported();
        return null;
    }

    @Override
    public List<PodLogHandler> logs(String tenant, String applicationId, LogOptions logOptions) {
        notSupported();
        return null;
    }

    @Override
    public String getExecutorServiceURI(String tenant, String applicationId, String executorId) {
        notSupported();
        return null;
    }

    @Override
    public String storeType() {
        return "memory";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {}

    private void notSupported() {
        throw new UnsupportedOperationException();
    }
}
