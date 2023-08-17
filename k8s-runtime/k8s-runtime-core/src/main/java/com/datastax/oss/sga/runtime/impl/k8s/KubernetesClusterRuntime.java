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
package com.datastax.oss.sga.runtime.impl.k8s;

import ai.langstream.api.model.ErrorsSpec;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.ExecutionPlanOptimiser;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentSpec;
import com.datastax.oss.sga.deployer.k8s.apps.AppResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.agents.ai.GenAIToolKitExecutionPlanOptimizer;
import ai.langstream.impl.common.BasicClusterRuntime;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import ai.langstream.impl.agents.ComposableAgentExecutionPlanOptimiser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.security.MessageDigest;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class KubernetesClusterRuntime extends BasicClusterRuntime {
    static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    public static final String CLUSTER_TYPE = "kubernetes";

    static final List<ExecutionPlanOptimiser> OPTIMISERS = List.of(
            new ComposableAgentExecutionPlanOptimiser(),
            new GenAIToolKitExecutionPlanOptimizer());

    private KubernetesClusterRuntimeConfiguration configuration;
    private KubernetesClient client;

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        this.configuration = mapper.convertValue(configuration, KubernetesClusterRuntimeConfiguration.class);
        this.client = KubernetesClientFactory.create(null);
    }

    @Override
    @SneakyThrows
    public Object deploy(String tenant,
                         ExecutionPlan applicationInstance,
                         StreamingClusterRuntime streamingClusterRuntime,
                         String codeStorageArchiveId) {
        streamingClusterRuntime.deploy(applicationInstance);
        List<AgentCustomResource> agentCustomResources = new ArrayList<>();
        List<Secret> secrets = new ArrayList<>();
        collectAgentCustomResourcesAndSecrets(tenant, agentCustomResources, secrets, applicationInstance, streamingClusterRuntime,
                        codeStorageArchiveId);
        final String namespace = computeNamespace(tenant);

        for (Secret secret : secrets) {
            client.resource(secret).inNamespace(namespace).serverSideApply();
            log.info("Created secret for agent {}",
                    secret.getMetadata().getName());
        }

        for (AgentCustomResource agentCustomResource : agentCustomResources) {
            client.resource(agentCustomResource).inNamespace(namespace).serverSideApply();
            log.info("Created custom resource for agent {}",
                    agentCustomResource.getMetadata().getName());
        }
        return null;
    }

    private void collectAgentCustomResourcesAndSecrets(
            String tenant,
            List<AgentCustomResource> agentsCustomResourceDefinitions,
            List<Secret> secrets,
            ExecutionPlan applicationInstance,
            StreamingClusterRuntime streamingClusterRuntime,
            String codeStorageArchiveId) {
        for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
            collectAgentCustomResourceAndSecret(tenant, agentsCustomResourceDefinitions,
                    secrets, agentImplementation, streamingClusterRuntime,
                    applicationInstance, codeStorageArchiveId);
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
            String codeStorageArchiveId) {
        log.info("Building configuration for Agent {}, codeStorageArchiveId {}", agent, codeStorageArchiveId);
        if (!(agent instanceof DefaultAgentNode)) {
            throw new UnsupportedOperationException("Only default agent implementations are supported");
        }
        DefaultAgentNode defaultAgentImplementation = (DefaultAgentNode) agent;

        Map<String, Object> inputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getInputConnectionImplementation() != null) {
            inputConfiguration = streamingClusterRuntime.createConsumerConfiguration(defaultAgentImplementation,
                    defaultAgentImplementation.getInputConnectionImplementation());
        }
        Map<String, Object> outputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getOutputConnectionImplementation() != null) {
            outputConfiguration = streamingClusterRuntime.createProducerConfiguration(defaultAgentImplementation,
                    defaultAgentImplementation.getOutputConnectionImplementation());
        }

        final String secretName =
                AgentResourcesFactory.getAgentCustomResourceName(applicationInstance.getApplicationId(), agent.getId());


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

        RuntimePodConfiguration podConfig = new RuntimePodConfiguration(
                inputConfiguration,
                outputConfiguration,
                new com.datastax.oss.sga.runtime.api.agent.AgentSpec(
                        com.datastax.oss.sga.runtime.api.agent.AgentSpec.ComponentType.valueOf(
                                defaultAgentImplementation.getComponentType().name()
                        ),
                        tenant,
                        defaultAgentImplementation.getId(),
                        applicationInstance.getApplicationId(),
                        defaultAgentImplementation.getAgentType(),
                        defaultAgentImplementation.getConfiguration(),
                        errorsConfiguration
                ),
                applicationInstance.getApplication().getInstance().streamingCluster()
        );


        final Secret secret = AgentResourcesFactory.generateAgentSecret(
                AgentResourcesFactory.getAgentCustomResourceName(applicationInstance.getApplicationId(), agent.getId()),
                podConfig);

        final AgentSpec agentSpec = new AgentSpec();
        agentSpec.setTenant(tenant);
        agentSpec.setApplicationId(applicationInstance.getApplicationId());
        agentSpec.setImage(configuration.getImage());
        agentSpec.setImagePullPolicy(configuration.getImagePullPolicy());
        agentSpec.setResources(new AgentSpec.Resources(
                ((DefaultAgentNode) agent).getResourcesSpec().parallelism(),
                ((DefaultAgentNode) agent).getResourcesSpec().size()
        ));
        agentSpec.setAgentConfigSecretRef(secretName);
        agentSpec.setCodeArchiveId(codeStorageArchiveId);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(SerializationUtil.writeAsJsonBytes(secret.getData()));
        agentSpec.setAgentConfigSecretRefChecksum(bytesToHex(hash));


        final AgentCustomResource agentCustomResource = AgentResourcesFactory.generateAgentCustomResource(
                applicationInstance.getApplicationId(),
                agent.getId(),
                agentSpec
        );

        agentsCustomResourceDefinitions.add(agentCustomResource);
        secrets.add(secret);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    @Override
    protected void validateExecutionPlan(ExecutionPlan plan, StreamingClusterRuntime streamingClusterRuntime)
            throws IllegalArgumentException {
        super.validateExecutionPlan(plan, streamingClusterRuntime);
        AppResourcesFactory.validateApplicationId(plan.getApplicationId());
        final String applicationId = plan.getApplicationId();
        for (AgentNode agentNode : plan.getAgents().values()) {
            AgentResourcesFactory.validateAgentId(agentNode.getId(), applicationId);
        }
    }

    @Override
    public void delete(String tenant, ExecutionPlan applicationInstance,
                       StreamingClusterRuntime streamingClusterRuntime, String codeStorageArchiveId) {
        List<AgentCustomResource> agentCustomResources = new ArrayList<>();
        List<Secret> secrets = new ArrayList<>();
        collectAgentCustomResourcesAndSecrets(tenant, agentCustomResources, secrets, applicationInstance, streamingClusterRuntime,
                codeStorageArchiveId);
        final String namespace = computeNamespace(tenant);

        for (Secret secret : secrets) {
            client.resource(secret).inNamespace(namespace).delete();
            log.info("Deleted secret for agent {}",
                    secret.getMetadata().getName());
        }

        for (AgentCustomResource agentCustomResource : agentCustomResources) {
            client.resource(agentCustomResource).inNamespace(namespace).delete();
            log.info("Delete custom resource for agent {}",
                    agentCustomResource.getMetadata().getName());
        }
    }

    private String computeNamespace(String tenant) {
        final String namespace = configuration.getNamespacePrefix() + tenant;
        return namespace;
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
