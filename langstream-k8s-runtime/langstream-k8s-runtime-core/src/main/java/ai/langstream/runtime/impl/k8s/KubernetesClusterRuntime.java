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
package ai.langstream.runtime.impl.k8s;

import ai.langstream.api.model.DiskSpec;
import ai.langstream.api.model.ErrorsSpec;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.ExecutionPlanOptimiser;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.webservice.application.ApplicationCodeInfo;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.agents.AgentSpec;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.agents.ComposableAgentExecutionPlanOptimiser;
import ai.langstream.impl.common.BasicClusterRuntime;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesClusterRuntime extends BasicClusterRuntime {
    static final ObjectMapper mapper =
            new ObjectMapper()
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    public static final String CLUSTER_TYPE = "kubernetes";

    static final List<ExecutionPlanOptimiser> OPTIMISERS =
            List.of(new ComposableAgentExecutionPlanOptimiser());

    private static final MessageDigest DIGEST;

    static {
        try {
            DIGEST = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private KubernetesClusterRuntimeConfiguration configuration;
    private KubernetesClient client;

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        this.configuration =
                mapper.convertValue(configuration, KubernetesClusterRuntimeConfiguration.class);
        this.client = KubernetesClientFactory.create(null);
    }

    @Override
    @SneakyThrows
    public Object deploy(
            String tenant,
            ExecutionPlan executionPlan,
            StreamingClusterRuntime streamingClusterRuntime,
            String codeStorageArchiveId,
            DeployContext deployContext) {

        // this code is executed in the application setup job, that is a process with admin
        // privileges
        // this code must never run custom code imported from the application (like Jars or Py).

        List<AgentCustomResource> agentCustomResources = new ArrayList<>();
        List<Secret> secrets = new ArrayList<>();
        collectAgentCustomResourcesAndSecrets(
                tenant,
                agentCustomResources,
                secrets,
                executionPlan,
                streamingClusterRuntime,
                codeStorageArchiveId,
                deployContext);
        final String namespace = computeNamespace(tenant);

        for (Secret secret : secrets) {
            client.resource(secret).inNamespace(namespace).serverSideApply();
            log.info("Created secret for agent {}", secret.getMetadata().getName());
        }

        for (AgentCustomResource agentCustomResource : agentCustomResources) {
            final AgentCustomResource existing =
                    client.resource(agentCustomResource).inNamespace(namespace).get();
            if (existing != null) {
                final String codeArchiveId =
                        tryKeepPreviousCodeArchiveId(
                                tenant,
                                executionPlan.getApplicationId(),
                                codeStorageArchiveId,
                                existing.getSpec().getCodeArchiveId(),
                                deployContext);
                agentCustomResource.getSpec().setCodeArchiveId(codeArchiveId);
            }
            client.resource(agentCustomResource).inNamespace(namespace).serverSideApply();
            log.info(
                    "Patched custom resource for agent {}",
                    agentCustomResource.getMetadata().getName());
        }
        return null;
    }

    static String tryKeepPreviousCodeArchiveId(
            String tenant,
            String applicationId,
            String newCodeStorageArchiveId,
            String currentCodeStorageArchiveId,
            DeployContext deployContext) {
        final Map<String, ApplicationCodeInfo> archiveInfoLocalCache = new HashMap<>();
        final boolean digestChanged =
                isDigestChanged(
                        tenant,
                        applicationId,
                        newCodeStorageArchiveId,
                        deployContext,
                        currentCodeStorageArchiveId,
                        archiveInfoLocalCache);
        if (!digestChanged) {
            log.info(
                    "Digest for agent {} is the same (current code archive id {}, new archive id {}).",
                    applicationId,
                    currentCodeStorageArchiveId,
                    newCodeStorageArchiveId);
            return currentCodeStorageArchiveId;
        } else {
            log.info(
                    "Digest for agent {} changed (current code archive id {}, new archive id {}).",
                    applicationId,
                    currentCodeStorageArchiveId,
                    newCodeStorageArchiveId);
            return newCodeStorageArchiveId;
        }
    }

    private static boolean isDigestChanged(
            String tenant,
            String applicationId,
            String newCodeArchiveId,
            DeployContext deployContext,
            String currentCodeArchiveId,
            Map<String, ApplicationCodeInfo> cache) {
        final ApplicationCodeInfo currentCodeInfo =
                getCodeInfo(cache, currentCodeArchiveId, deployContext, tenant, applicationId);
        if (currentCodeInfo == null) {
            log.warn(
                    "No code info for {} {}. Will update agents archive id anyway.",
                    tenant,
                    currentCodeArchiveId);
            return true;
        }
        final ApplicationCodeInfo newCodeInfo =
                getCodeInfo(cache, newCodeArchiveId, deployContext, tenant, applicationId);
        if (newCodeInfo == null) {
            log.warn(
                    "No code info for {} {}. Will update agents archive id anyway.",
                    tenant,
                    newCodeArchiveId);
            return true;
        }
        final ApplicationCodeInfo.Digests newDigests = newCodeInfo.getDigests();
        final ApplicationCodeInfo.Digests currentDigests = currentCodeInfo.getDigests();
        if (newDigests == null && currentDigests == null) {
            return false;
        }
        if (newDigests == null || currentDigests == null) {
            return true;
        }
        if (!Objects.equals(newDigests.getPython(), currentDigests.getPython())) {
            return true;
        }
        if (!Objects.equals(newDigests.getJava(), currentDigests.getJava())) {
            return true;
        }
        return false;
    }

    private static ApplicationCodeInfo getCodeInfo(
            Map<String, ApplicationCodeInfo> cache,
            String currentCodeArchiveId,
            DeployContext deployContext,
            String tenant,
            String applicationId) {
        return cache.computeIfAbsent(
                currentCodeArchiveId,
                id -> deployContext.getApplicationCodeInfo(tenant, applicationId, id));
    }

    private void collectAgentCustomResourcesAndSecrets(
            String tenant,
            List<AgentCustomResource> agentsCustomResourceDefinitions,
            List<Secret> secrets,
            ExecutionPlan applicationInstance,
            StreamingClusterRuntime streamingClusterRuntime,
            String codeStorageArchiveId,
            DeployContext deployContext) {
        for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
            collectAgentCustomResourceAndSecret(
                    tenant,
                    agentsCustomResourceDefinitions,
                    secrets,
                    agentImplementation,
                    streamingClusterRuntime,
                    applicationInstance,
                    codeStorageArchiveId,
                    deployContext);
        }
    }

    @SneakyThrows
    private void collectAgentCustomResourceAndSecret(
            String tenant,
            List<AgentCustomResource> agentsCustomResourceDefinitions,
            List<Secret> secrets,
            AgentNode agent,
            StreamingClusterRuntime streamingClusterRuntime,
            ExecutionPlan applicationInstance,
            String codeStorageArchiveId,
            DeployContext deployContext) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Building configuration for Agent {}, codeStorageArchiveId {}",
                    agent,
                    codeStorageArchiveId);
        }
        if (!(agent instanceof DefaultAgentNode defaultAgentImplementation)) {
            throw new UnsupportedOperationException(
                    "Only default agent implementations are supported");
        }

        Map<String, Object> inputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getInputConnectionImplementation() != null) {
            inputConfiguration =
                    streamingClusterRuntime.createConsumerConfiguration(
                            defaultAgentImplementation,
                            defaultAgentImplementation.getInputConnectionImplementation());
        }
        Map<String, Object> outputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getOutputConnectionImplementation() != null) {
            outputConfiguration =
                    streamingClusterRuntime.createProducerConfiguration(
                            defaultAgentImplementation,
                            defaultAgentImplementation.getOutputConnectionImplementation());
        }

        final String secretName =
                AgentResourcesFactory.getAgentCustomResourceName(
                        applicationInstance.getApplicationId(), agent.getId());

        Map<String, Object> errorsConfiguration = new HashMap<>();
        ErrorsSpec errorsSpec = defaultAgentImplementation.getErrorsSpec();
        if (errorsSpec == null) {
            errorsSpec = ErrorsSpec.DEFAULT;
        } else {
            errorsSpec = errorsSpec.withDefaultsFrom(ErrorsSpec.DEFAULT);
        }
        // set StandardErrorHandler
        errorsConfiguration.put("retries", errorsSpec.getRetries());
        errorsConfiguration.put("onFailure", errorsSpec.getOnFailure());
        Set<String> agentIdsWithDisks =
                defaultAgentImplementation.getDisks() != null
                        ? defaultAgentImplementation.getDisks().keySet()
                        : null;

        final StreamingCluster streamingCluster =
                applicationInstance.getApplication().getInstance().streamingCluster();
        RuntimePodConfiguration podConfig =
                new RuntimePodConfiguration(
                        inputConfiguration,
                        outputConfiguration,
                        new ai.langstream.runtime.api.agent.AgentSpec(
                                ai.langstream.runtime.api.agent.AgentSpec.ComponentType.valueOf(
                                        defaultAgentImplementation.getComponentType().name()),
                                tenant,
                                defaultAgentImplementation.getId(),
                                applicationInstance.getApplicationId(),
                                defaultAgentImplementation.getAgentType(),
                                defaultAgentImplementation.getConfiguration(),
                                errorsConfiguration,
                                agentIdsWithDisks != null ? agentIdsWithDisks : Set.of()),
                        streamingCluster);

        final Secret secret =
                AgentResourcesFactory.generateAgentSecret(
                        AgentResourcesFactory.getAgentCustomResourceName(
                                applicationInstance.getApplicationId(), agent.getId()),
                        podConfig);

        final AgentSpec agentSpec = new AgentSpec();
        agentSpec.setTenant(tenant);
        agentSpec.setApplicationId(applicationInstance.getApplicationId());
        ResourcesSpec resourcesSpec = agent.getResources();
        List<AgentSpec.Disk> disks;
        if (agent.getDisks() != null && !agent.getDisks().isEmpty()) {
            disks = new ArrayList<>();
            agent.getDisks()
                    .forEach(
                            (k, v) -> {
                                disks.add(
                                        new AgentSpec.Disk(
                                                k, DiskSpec.parseSize(v.size()), v.type()));
                            });
        } else {
            disks = List.of();
        }

        agentSpec.setResources(
                new AgentSpec.Resources(resourcesSpec.parallelism(), resourcesSpec.size()));
        AgentSpec.Options options =
                new AgentSpec.Options(
                        disks,
                        deployContext.isAutoUpgradeRuntimeImage(),
                        deployContext.isAutoUpgradeRuntimeImagePullPolicy(),
                        deployContext.isAutoUpgradeAgentResources(),
                        deployContext.isAutoUpgradeAgentPodTemplate(),
                        deployContext.getApplicationSeed());
        agentSpec.serializeAndSetOptions(options);
        agentSpec.setAgentConfigSecretRef(secretName);
        agentSpec.setCodeArchiveId(codeStorageArchiveId);
        byte[] hash = DIGEST.digest(SerializationUtil.writeAsJsonBytes(secret.getData()));
        agentSpec.setAgentConfigSecretRefChecksum(bytesToHex(hash));

        final AgentCustomResource agentCustomResource =
                AgentResourcesFactory.generateAgentCustomResource(
                        applicationInstance.getApplicationId(), agent.getId(), agentSpec);

        agentsCustomResourceDefinitions.add(agentCustomResource);
        secrets.add(secret);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    @Override
    protected void validateExecutionPlan(
            ExecutionPlan plan, StreamingClusterRuntime streamingClusterRuntime)
            throws IllegalArgumentException {
        super.validateExecutionPlan(plan, streamingClusterRuntime);
        AppResourcesFactory.validateApplicationId(plan.getApplicationId());
        final String applicationId = plan.getApplicationId();
        for (AgentNode agentNode : plan.getAgents().values()) {
            AgentResourcesFactory.validateAgentId(agentNode.getId(), applicationId);
        }
    }

    @Override
    public void delete(
            String tenant,
            ExecutionPlan applicationInstance,
            StreamingClusterRuntime streamingClusterRuntime,
            String codeStorageArchiveId,
            DeployContext deployContext) {
        List<AgentCustomResource> agentCustomResources = new ArrayList<>();
        List<Secret> secrets = new ArrayList<>();
        collectAgentCustomResourcesAndSecrets(
                tenant,
                agentCustomResources,
                secrets,
                applicationInstance,
                streamingClusterRuntime,
                codeStorageArchiveId,
                deployContext);
        final String namespace = computeNamespace(tenant);

        for (Secret secret : secrets) {
            client.resource(secret).inNamespace(namespace).delete();
            log.info("Deleted secret for agent {}", secret.getMetadata().getName());
        }

        for (AgentCustomResource agentCustomResource : agentCustomResources) {
            client.resource(agentCustomResource).inNamespace(namespace).delete();
            log.info(
                    "Delete custom resource for agent {}",
                    agentCustomResource.getMetadata().getName());
        }
    }

    private String computeNamespace(String tenant) {
        return configuration.getNamespacePrefix() + tenant;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public List<ExecutionPlanOptimiser> getExecutionPlanOptimisers() {
        return OPTIMISERS;
    }
}
