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
package ai.langstream.impl.storage.k8s.apps;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.ApplicationStatus;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.deployer.k8s.api.crds.apps.SerializedApplicationInstance;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.limits.ApplicationResourceLimitsChecker;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class KubernetesApplicationStore implements ApplicationStore {

    private static final String[] LOG_COLORS =
            new String[] {"32", "33", "34", "35", "36", "37", "38"};
    protected static final String SECRET_KEY = "secrets";

    private static final ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private KubernetesClient client;
    private KubernetesApplicationStoreProperties properties;

    @Override
    public String storeType() {
        return "kubernetes";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        this.properties =
                mapper.convertValue(configuration, KubernetesApplicationStoreProperties.class);
        this.client = KubernetesClientFactory.get(null);
    }

    private String tenantToNamespace(String tenant) {
        return properties.getNamespaceprefix() + tenant;
    }

    @Override
    @SneakyThrows
    public void onTenantCreated(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        new TenantResources(properties, client, tenant, namespace).ensureTenantResources();
    }

    @Override
    public void onTenantUpdated(String tenant) {}

    @Override
    public void onTenantDeleted(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        if (client.namespaces().withName(namespace).get() != null) {
            client.namespaces().withName(namespace).delete();
        }
    }

    @Override
    @SneakyThrows
    public void put(
            String tenant,
            String applicationId,
            Application applicationInstance,
            String codeArchiveId,
            ExecutionPlan executionPlan) {
        final ApplicationCustomResource existing =
                getApplicationCustomResource(tenant, applicationId);
        if (existing != null) {
            if (existing.isMarkedForDeletion()) {
                throw new IllegalArgumentException(
                        "Application "
                                + applicationId
                                + " is marked for deletion. Please retry once the application is deleted.");
            }
        }

        final String namespace = tenantToNamespace(tenant);

        ApplicationCustomResource crd = new ApplicationCustomResource();
        crd.setMetadata(
                new ObjectMetaBuilder().withName(applicationId).withNamespace(namespace).build());
        final SerializedApplicationInstance serializedApp =
                new SerializedApplicationInstance(applicationInstance, executionPlan);
        final ApplicationSpec spec =
                ApplicationSpec.builder()
                        .tenant(tenant)
                        .application(ApplicationSpec.serializeApplication(serializedApp))
                        .codeArchiveId(codeArchiveId)
                        .build();
        log.info(
                "Creating application {} in namespace {}, spec {}", applicationId, namespace, spec);
        crd.setSpec(spec);

        client.resource(crd).inNamespace(namespace).serverSideApply();

        // need to refresh to get the uid
        crd = client.resource(crd).inNamespace(namespace).get();

        final Secret secret =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(applicationId)
                        .withNamespace(namespace)
                        .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(crd))
                        .endMetadata()
                        .withData(
                                Map.of(
                                        SECRET_KEY,
                                        encodeSecret(
                                                mapper.writeValueAsString(
                                                        applicationInstance.getSecrets()))))
                        .build();
        client.resource(secret).inNamespace(namespace).serverSideApply();
    }

    @Override
    public StoredApplication get(String tenant, String applicationId, boolean queryPods) {
        final ApplicationCustomResource application =
                getApplicationCustomResource(tenant, applicationId);
        if (application == null) {
            return null;
        }
        return getApplicationStatus(applicationId, application, queryPods);
    }

    @Override
    public ApplicationSpecs getSpecs(String tenant, String applicationId) {
        final ApplicationCustomResource application =
                getApplicationCustomResource(tenant, applicationId);
        if (application == null) {
            return null;
        }
        SerializedApplicationInstance serializedApplicationInstanceFromCr =
                getSerializedApplicationInstanceFromCr(application);
        final Application app = serializedApplicationInstanceFromCr.toApplicationInstance();
        return ApplicationSpecs.builder()
                .application(app)
                .codeArchiveReference(application.getSpec().getCodeArchiveId())
                .applicationId(applicationId)
                .build();
    }

    @Override
    @SneakyThrows
    public Secrets getSecrets(String tenant, String applicationId) {
        final Secret secret =
                client.secrets()
                        .inNamespace(tenantToNamespace(tenant))
                        .withName(applicationId)
                        .get();
        if (secret == null) {
            return null;
        }

        if (secret.getData() != null && secret.getData().get(SECRET_KEY) != null) {
            final String s = secret.getData().get(SECRET_KEY);
            final String decoded =
                    new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
            return mapper.readValue(decoded, Secrets.class);
        }
        return null;
    }

    @Nullable
    private ApplicationCustomResource getApplicationCustomResource(
            String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);
        return client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .get();
    }

    @Override
    public void delete(String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);

        client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .delete();
        // the secret deletion will happen automatically once the app custom resource has been
        // deleted completely
    }

    @Override
    public Map<String, StoredApplication> list(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return client
                .resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .list()
                .getItems()
                .stream()
                .map(a -> getApplicationStatus(a.getMetadata().getName(), a, false))
                .collect(
                        Collectors.toMap(StoredApplication::getApplicationId, Function.identity()));
    }

    @Override
    public Map<String, Integer> getResourceUsage(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return ApplicationResourceLimitsChecker.loadUsage(client, namespace);
    }

    @SneakyThrows
    private StoredApplication getApplicationStatus(
            String applicationId, ApplicationCustomResource application, boolean queryPods) {
        SerializedApplicationInstance serializedApplicationInstanceFromCr =
                getSerializedApplicationInstanceFromCr(application);
        final Application instance = serializedApplicationInstanceFromCr.toApplicationInstance();
        final Map<String, SerializedApplicationInstance.AgentRunnerDefinition> agentNodes =
                serializedApplicationInstanceFromCr.getAgentRunners();

        return StoredApplication.builder()
                .applicationId(applicationId)
                .instance(instance)
                .codeArchiveReference(application.getSpec().getCodeArchiveId())
                .status(computeApplicationStatus(applicationId, agentNodes, application, queryPods))
                .build();
    }

    private SerializedApplicationInstance getSerializedApplicationInstanceFromCr(
            ApplicationCustomResource application) {
        return ApplicationSpec.deserializeApplication(application.getSpec().getApplication());
    }

    private ApplicationStatus computeApplicationStatus(
            final String applicationId,
            Map<String, SerializedApplicationInstance.AgentRunnerDefinition> agentRunners,
            ApplicationCustomResource customResource,
            boolean queryPods) {
        log.info(
                "Computing status for application {}, agentRunners {}",
                applicationId,
                agentRunners);
        final ApplicationStatus result = new ApplicationStatus();
        result.setStatus(AppResourcesFactory.computeApplicationStatus(client, customResource));
        List<String> declaredAgents = new ArrayList<>();

        Map<String, AgentResourcesFactory.AgentRunnerSpec> agentRunnerSpecMap = new HashMap<>();
        agentRunners.forEach(
                (agentId, agentRunnerDefinition) -> {
                    log.info("Adding agent id {} (def {})", agentId, agentRunnerDefinition);
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

        result.setAgents(
                AgentResourcesFactory.aggregateAgentsStatus(
                        client,
                        customResource.getMetadata().getNamespace(),
                        applicationId,
                        declaredAgents,
                        agentRunnerSpecMap,
                        queryPods));
        return result;
    }

    static String encodeSecret(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    @SneakyThrows
    public List<PodLogHandler> logs(String tenant, String applicationId, LogOptions logOptions) {
        final List<String> pods =
                AgentResourcesFactory.getAgentPods(
                        client, tenantToNamespace(tenant), applicationId);

        return pods.stream()
                .filter(
                        pod -> {
                            if (logOptions.getFilterReplicas() != null
                                    && !logOptions.getFilterReplicas().isEmpty()) {
                                return logOptions.getFilterReplicas().contains(pod);
                            }
                            return true;
                        })
                .map(pod -> new StreamPodLogHandler(tenant, pod))
                .collect(Collectors.toList());
    }

    private int extractReplicas(String pod) {
        final String[] split = pod.split("-");
        return Integer.parseInt(split[split.length - 1]);
    }

    private class StreamPodLogHandler implements PodLogHandler {
        private final String tenant;
        private final String pod;
        private InputStream in;
        private volatile boolean closed;

        public StreamPodLogHandler(String tenant, String pod) {
            this.tenant = tenant;
            this.pod = pod;
        }

        @Override
        public String getPodName() {
            return pod;
        }

        @Override
        @SneakyThrows
        public void start(LogLineConsumer onLogLine) {
            while (true) {
                try {
                    boolean continueLoop = streamUntilEOF(onLogLine);
                    if (!continueLoop) {
                        return;
                    }
                    if (closed) {
                        return;
                    }
                } catch (Throwable tt) {
                    if (!closed) {
                        log.warn("Error while streaming logs for pod {}", pod, tt);
                    }
                    onLogLine.onEnd();
                }
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                in.close();
            } catch (Throwable ignored) {
            }
        }

        private boolean streamUntilEOF(LogLineConsumer onLogLine) throws IOException {
            final PodResource podResource =
                    client.pods().inNamespace(tenantToNamespace(tenant)).withName(pod);
            final int replicas = extractReplicas(pod);
            final String color = LOG_COLORS[replicas % LOG_COLORS.length];
            try (final LogWatch watchLog =
                    podResource
                            .inContainer("runtime")
                            .withPrettyOutput()
                            .withReadyWaitTimeout(Integer.MAX_VALUE)
                            .watchLog(); ) {
                in = watchLog.getOutput();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String coloredLog =
                                "\u001B[%sm[%s] %s\u001B[0m\n".formatted(color, pod, line);
                        final boolean shallContinue = onLogLine.onLogLine(coloredLog);
                        if (!shallContinue) {
                            return false;
                        }
                        if (closed) {
                            return false;
                        }
                    }
                }
            }
            return onLogLine.onLogLine("[%s] EOF\n".formatted(pod));
        }
    }
}
