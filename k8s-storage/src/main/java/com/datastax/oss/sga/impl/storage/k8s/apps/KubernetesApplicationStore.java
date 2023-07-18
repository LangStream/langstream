package com.datastax.oss.sga.impl.storage.k8s.apps;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.ApplicationStatus;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.apps.AppResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.impl.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesApplicationStore implements ApplicationStore {

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
    public void put(String tenant, String applicationId, Application applicationInstance, String codeArchiveId) {
        final String namespace = tenantToNamespace(tenant);
        final String appJson = mapper.writeValueAsString(new SerializedApplicationInstance(applicationInstance));
        final ApplicationCustomResource crd = new ApplicationCustomResource();
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
        crd.setSpec(spec);

        client.resource(crd)
                .inNamespace(namespace)
                .serverSideApply();

        final Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(applicationId)
                .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of("secrets",
                        encodeSecret(mapper.writeValueAsString(applicationInstance.getSecrets()))))
                .build();
        client.resource(secret)
                .inNamespace(namespace)
                .serverSideApply();
    }

    @Override
    public StoredApplication get(String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);
        final ApplicationCustomResource application = client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .get();
        if (application == null) {
            return null;
        }
        return convertApplicationToResult(applicationId, application);
    }

    @Override
    public void delete(String tenant, String applicationId) {
        final String namespace = tenantToNamespace(tenant);

        client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .delete();
        client.resources(Secret.class)
                .inNamespace(namespace)
                .withName(applicationId)
                .delete();
    }

    @Override
    public Map<String, StoredApplication> list(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return client.resources(ApplicationCustomResource.class)
                .inNamespace(namespace)
                .list()
                .getItems().stream()
                .map(a -> convertApplicationToResult(a.getMetadata().getName(), a))
                .collect(Collectors.toMap(StoredApplication::getApplicationId, Function.identity()));
    }

    @SneakyThrows
    private StoredApplication convertApplicationToResult(String applicationId,
                                                         ApplicationCustomResource application) {
        final Application instance =
                mapper.readValue(application.getSpec().getApplication(), SerializedApplicationInstance.class)
                        .toApplicationInstance();

        // TODO: load secrets ?

        return StoredApplication.builder()
                .applicationId(applicationId)
                .instance(instance)
                .status(computeApplicationStatus(applicationId, instance, application))
                .build();
    }


    private ApplicationStatus computeApplicationStatus(final String applicationId, Application app,
                                                       ApplicationCustomResource customResource) {
        final ApplicationStatus result = new ApplicationStatus();
        result.setStatus(AppResourcesFactory.computeApplicationStatus(client, customResource));
        List<String> declaredAgents = new ArrayList<>();
        for (Module module : app.getModules().values()) {
            for (Pipeline pipeline : module.getPipelines().values()) {
                for (AgentConfiguration agent : pipeline.getAgents()) {
                    declaredAgents.add(agent.getId());
                }
            }
        }
        result.setAgents(AgentResourcesFactory.aggregateAgentsStatus(client, customResource
                .getMetadata().getNamespace(), applicationId, declaredAgents));
        return result;
    }


    @Data
    @NoArgsConstructor
    public static class SerializedApplicationInstance {

        public SerializedApplicationInstance(Application applicationInstance) {
            this.resources = applicationInstance.getResources();
            this.modules = applicationInstance.getModules();
            this.instance = applicationInstance.getInstance();
        }

        private Map<String, Resource> resources = new HashMap<>();
        private Map<String, Module> modules = new HashMap<>();
        private Instance instance;

        public Application toApplicationInstance() {
            final Application app = new Application();
            app.setInstance(instance);
            app.setModules(modules);
            app.setResources(resources);
            return app;
        }

    }

    private static String encodeSecret(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
