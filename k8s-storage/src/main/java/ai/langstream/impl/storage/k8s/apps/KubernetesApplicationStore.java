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
package ai.langstream.impl.storage.k8s.apps;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationStatus;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.Instance;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import ai.langstream.api.storage.ApplicationStore;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.apps.AppResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import ai.langstream.impl.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import java.io.BufferedReader;
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
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class KubernetesApplicationStore implements ApplicationStore {

    private static final String[] LOG_COLORS = new String[]{"32", "33", "34", "35", "36", "37", "38"};
    protected static final String SECRET_KEY = "secrets";

    private static ObjectMapper mapper = new ObjectMapper();
    private KubernetesClient client;
    private KubernetesApplicationStoreProperties properties;


    @Override
    public String storeType() {
        return "kubernetes";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        final KubernetesApplicationStoreProperties props =
                mapper.convertValue(configuration, KubernetesApplicationStoreProperties.class);
        this.properties = props;
        this.client = KubernetesClientFactory.get(null);
    }

    private String tenantToNamespace(String tenant) {
        return properties.getNamespaceprefix() + tenant;
    }


    @Override
    public void onTenantCreated(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        if (client.namespaces().withName(namespace).get() == null) {
            client.resource(new NamespaceBuilder()
                            .withNewMetadata()
                            .withName(namespace)
                            .endMetadata().build())
                    .serverSideApply();
            log.info("Created namespace {} for tenant {}", namespace, tenant);

            client.resource(new ServiceAccountBuilder()
                            .withNewMetadata()
                            .withName(tenant)
                            .endMetadata()
                            .build())
                    .inNamespace(namespace)
                    .serverSideApply();
            log.info("Created service account for tenant {}", tenant);

            client.resource(new RoleBuilder()
                            .withNewMetadata()
                            .withName(tenant)
                            .endMetadata()
                            .withRules(new PolicyRuleBuilder()
                                    .withApiGroups("sga.oss.datastax.com")
                                    .withResources("agents")
                                    .withVerbs("*")
                                    .build(),
                                    new PolicyRuleBuilder()
                                            .withApiGroups("")
                                            .withResources("secrets")
                                            .withVerbs("*")
                                            .build()
                            ).build())
                    .inNamespace(namespace)
                    .serverSideApply();

            log.info("Created role for tenant {}", tenant);

            client.resource(new RoleBindingBuilder()
                            .withNewMetadata()
                            .withName(tenant)
                            .endMetadata()
                            .withNewRoleRef()
                            .withKind("Role")
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withName(tenant)
                            .endRoleRef()
                            .withSubjects(new SubjectBuilder()
                                    .withName(tenant)
                                    .withNamespace(namespace)
                                    .withKind("ServiceAccount")
                                    .build()
                            )
                            .build())
                    .inNamespace(namespace)
                    .serverSideApply();
            log.info("Created role binding for tenant {}", tenant);

        }
    }

    @Override
    public void onTenantDeleted(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        if (client.namespaces().withName(namespace).get() != null) {
            client.namespaces().withName(namespace).delete();
        }
    }

    @Override
    @SneakyThrows
    public void put(String tenant, String applicationId, Application applicationInstance, String codeArchiveId, ExecutionPlan executionPlan) {
        final ApplicationCustomResource existing = getApplicationCustomResource(tenant, applicationId);
        if (existing != null) {
            if (existing.isMarkedForDeletion()) {
                throw new IllegalArgumentException("Application " + applicationId + " is marked for deletion. Please retry once the application is deleted.");
            }
        }

        final String namespace = tenantToNamespace(tenant);
        final String appJson = mapper.writeValueAsString(new SerializedApplicationInstance(applicationInstance, executionPlan));
        ApplicationCustomResource crd = new ApplicationCustomResource();
        crd.setMetadata(new ObjectMetaBuilder()
                .withName(applicationId)
                .withNamespace(namespace)
                .build());
        final ApplicationSpec spec = ApplicationSpec.builder()
                .tenant(tenant)
                .image(properties.getDeployerRuntime().getImage())
                .imagePullPolicy(properties.getDeployerRuntime().getImagePullPolicy())
                .application(appJson)
                .codeArchiveId(codeArchiveId)
                .build();
        log.info("Creating application {} in namespace {}, spec {}", applicationId, namespace, spec);
        crd.setSpec(spec);

        client.resource(crd)
                .inNamespace(namespace)
                .serverSideApply();

        // need to refresh to get the uid
        crd = client.resource(crd)
                .inNamespace(namespace)
                .get();

        final Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(applicationId)
                .withNamespace(namespace)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(crd))
                .endMetadata()
                .withData(Map.of(SECRET_KEY,
                        encodeSecret(mapper.writeValueAsString(applicationInstance.getSecrets()))))
                .build();
        client.resource(secret)
                .inNamespace(namespace)
                .serverSideApply();
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
    public Application getSpecs(String tenant, String applicationId) {
        final ApplicationCustomResource application =
                getApplicationCustomResource(tenant, applicationId);
        if (application == null) {
            return null;
        }
        SerializedApplicationInstance serializedApplicationInstanceFromCr = getSerializedApplicationInstanceFromCr(application);
        return serializedApplicationInstanceFromCr.toApplicationInstance();
    }

    @Override
    @SneakyThrows
    public Secrets getSecrets(String tenant, String applicationId) {
        final Secret secret = client.secrets().inNamespace(tenantToNamespace(tenant))
                .withName(applicationId)
                .get();
        if (secret == null) {
            return null;
        }

        if (secret.getData() != null && secret.getData().get(SECRET_KEY) != null) {
            final String s = secret.getData().get(SECRET_KEY);
            final String decoded = new String(Base64.getDecoder()
                    .decode(s), StandardCharsets.UTF_8);
            return mapper.readValue(decoded, Secrets.class);
        }
        return null;
    }

    @Nullable
    private ApplicationCustomResource getApplicationCustomResource(String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);
        final ApplicationCustomResource application = client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .get();
        if (application == null) {
            return null;
        }
        return application;
    }

    @Override
    public void delete(String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);

        client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .delete();
        // the secret deletion will happen automatically once the app custom resource has been deleted completely
    }

    @Override
    public Map<String, StoredApplication> list(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .list()
                .getItems().stream()
                .map(a -> getApplicationStatus(a.getMetadata().getName(), a, false))
                .collect(Collectors.toMap(StoredApplication::getApplicationId, Function.identity()));
    }

    @SneakyThrows
    private StoredApplication getApplicationStatus(String applicationId,
                                                   ApplicationCustomResource application,
                                                   boolean queryPods) {
        SerializedApplicationInstance serializedApplicationInstanceFromCr = getSerializedApplicationInstanceFromCr(application);
        final Application instance = serializedApplicationInstanceFromCr.toApplicationInstance();
        final Map<String, AgentRunnerDefinition> agentNodes = serializedApplicationInstanceFromCr.getAgentRunners();

        return StoredApplication.builder()
                .applicationId(applicationId)
                .instance(instance)
                .codeArchiveReference(application.getSpec().getCodeArchiveId())
                .status(computeApplicationStatus(applicationId,
                        agentNodes, application, queryPods))
                .build();
    }

    @SneakyThrows
    private SerializedApplicationInstance getSerializedApplicationInstanceFromCr(ApplicationCustomResource application) {
        return  mapper.readValue(application.getSpec().getApplication(), SerializedApplicationInstance.class);
    }


    private ApplicationStatus computeApplicationStatus(final String applicationId,
                                                       Map<String, AgentRunnerDefinition> agentRunners,
                                                       ApplicationCustomResource customResource,
                                                       boolean queryPods) {
        log.info("Computing status for application {}, agentRunners {}", applicationId, agentRunners);
        final ApplicationStatus result = new ApplicationStatus();
        result.setStatus(AppResourcesFactory.computeApplicationStatus(client, customResource));
        List<String> declaredAgents = new ArrayList<>();

        Map<String, AgentResourcesFactory.AgentRunnerSpec> agentRunnerSpecMap  = new HashMap<>();
        agentRunners.forEach((agentId, agentRunnerDefinition) -> {
            log.info("Adding agent id {} (def {})", agentId, agentRunnerDefinition);
            // this agentId doesn't contain the "module" prefix
            declaredAgents.add(agentRunnerDefinition.getAgentId());
            agentRunnerSpecMap.put(agentId, AgentResourcesFactory.AgentRunnerSpec.builder()
                    .agentId(agentRunnerDefinition.getAgentId())
                    .agentType(agentRunnerDefinition.getAgentType())
                    .componentType(agentRunnerDefinition.getComponentType())
                    .configuration(agentRunnerDefinition.getConfiguration())
                    .inputTopic(agentRunnerDefinition.getInputTopic())
                    .outputTopic(agentRunnerDefinition.getOutputTopic())
                    .build());
        });

        result.setAgents(AgentResourcesFactory.aggregateAgentsStatus(client, customResource
                .getMetadata().getNamespace(), applicationId, declaredAgents,
                agentRunnerSpecMap, queryPods));
        return result;
    }

    @Data
    public static class AgentRunnerDefinition {
        private String agentId;
        private String agentType;
        private String componentType;
        private Map<String, Object> configuration;
        private String inputTopic;
        private String outputTopic;
    }

    @Data
    @NoArgsConstructor
    public static class SerializedApplicationInstance {

        public SerializedApplicationInstance(Application applicationInstance, ExecutionPlan executionPlan) {
            this.resources = applicationInstance.getResources();
            this.modules = applicationInstance.getModules();
            this.instance = applicationInstance.getInstance();
            this.gateways = applicationInstance.getGateways();
            this.agentRunners = new HashMap<>();
            log.info("Serializing application instance {} executionPlan {}", applicationInstance, executionPlan);
            if (executionPlan != null && executionPlan.getAgents() != null) {
                for (Map.Entry<String, AgentNode> entry : executionPlan.getAgents().entrySet()) {
                    AgentNode agentNode = entry.getValue();
                    AgentRunnerDefinition agentRunnerDefinition = new AgentRunnerDefinition();
                    agentRunnerDefinition.setAgentId(agentNode.getId());
                    agentRunnerDefinition.setAgentType(agentNode.getAgentType());
                    agentRunnerDefinition.setComponentType(agentNode.getComponentType() + "");
                    agentRunnerDefinition.setConfiguration(agentNode.getConfiguration() != null ? agentNode.getConfiguration() : Map.of());
                    agentRunners.put(entry.getKey(), agentRunnerDefinition);

                    if (agentNode.getInputConnectionImplementation() instanceof Topic topic) {
                        agentRunnerDefinition.setInputTopic(topic.topicName());
                    }

                    if (agentNode.getOutputConnectionImplementation() instanceof Topic topic) {
                        agentRunnerDefinition.setOutputTopic(topic.topicName());
                    }

                }
            }
        }

        private Map<String, Resource> resources = new HashMap<>();
        private Map<String, Module> modules = new HashMap<>();
        private Instance instance;
        private Gateways gateways;
        private Map<String, AgentRunnerDefinition> agentRunners;

        public Application toApplicationInstance() {
            final Application app = new Application();
            app.setInstance(instance);
            app.setModules(modules);
            app.setResources(resources);
            app.setGateways(gateways);
            return app;
        }

        public Map<String, AgentRunnerDefinition> getAgentRunners() {
            return agentRunners;
        }
    }

    private static String encodeSecret(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }



    @Override
    @SneakyThrows
    public List<PodLogHandler> logs(String tenant, String applicationId, LogOptions logOptions) {
        final List<String> pods =
                AgentResourcesFactory.getAgentPods(client, tenantToNamespace(tenant), applicationId);

        return pods
                .stream()
                .filter(pod -> {
                    if (logOptions.getFilterReplicas() != null && !logOptions.getFilterReplicas().isEmpty()) {
                        return logOptions.getFilterReplicas().contains(pod);
                    }
                    return true;
                })
                .map(pod -> {
                    return new PodLogHandler() {
                        @Override
                        @SneakyThrows
                        public void start(LogLineConsumer onLogLine) {
                            final PodResource podResource =
                                    client.pods().inNamespace(tenantToNamespace(tenant)).withName(pod);
                            final int replicas = extractReplicas(pod);
                            final String color = LOG_COLORS[replicas % LOG_COLORS.length];
                            try (final LogWatch watchLog = podResource.inContainer("runtime")
                                    .withPrettyOutput().withReadyWaitTimeout(Integer.MAX_VALUE).watchLog();) {
                                final InputStream in = watchLog.getOutput();
                                try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                                    String line;
                                    while ((line = br.readLine()) != null) {
                                        String coloredLog = "\u001B[" + color + "m" + pod + " " + line + "\u001B[0m\n";
                                        final boolean shallContinue = onLogLine.onLogLine(coloredLog);
                                        if (!shallContinue) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    };
                })
                .collect(Collectors.toList());
    }

    private int extractReplicas(String pod) {
        final String[] split = pod.split("-");
        final int replicas = Integer.parseInt(split[split.length - 1]);
        return replicas;
    }
}
