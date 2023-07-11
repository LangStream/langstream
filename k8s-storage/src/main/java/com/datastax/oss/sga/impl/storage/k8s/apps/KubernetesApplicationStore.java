package com.datastax.oss.sga.impl.storage.k8s.apps;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.Application;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationStatus;
import com.datastax.oss.sga.deployer.k8s.client.DeployerClient;
import com.datastax.oss.sga.impl.storage.k8s.KubernetesClientFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.HashMap;
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
    private String namespacePrefix;


    @Override
    public String storeType() {
        return "kubernetes";
    }

    @Override
    public void initialize(Map<String, String> configuration) {
        final KubernetesApplicationStoreProperties props =
                mapper.convertValue(configuration, KubernetesApplicationStoreProperties.class);
        client = KubernetesClientFactory.get(null);
        namespacePrefix = props.getNamespaceprefix();
    }

    private String tenantToNamespace(String tenant) {
        return namespacePrefix + tenant;
    }


    @Override
    public void initializeTenant(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        if (client.namespaces().withName(namespace).get() == null) {
            client.resource(new NamespaceBuilder()
                            .withNewMetadata()
                            .withName(namespace)
                            .endMetadata().build())
                    .serverSideApply();
            log.info("Created namespace {} for tenant {}", namespace, tenant);
        }
    }

    @Override
    @SneakyThrows
    public void put(String tenant, String name, ApplicationInstance applicationInstance) {
        final String namespace = tenantToNamespace(tenant);
        final String appJson = mapper.writeValueAsString(new SerializedApplicationInstance(applicationInstance));
        final Application crd = new Application();
        crd.setMetadata(new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace)
                .build());
        final ApplicationSpec spec = ApplicationSpec.builder()
                .image("datastax/sga-runtime:latest")
                .imagePullPolicy("Always")
                .application(appJson)
                .build();
        crd.setSpec(spec);

        client.resource(crd)
                .inNamespace(namespace)
                .serverSideApply();
    }

    @Override
    public StoredApplicationInstance get(String tenant, String name) {
        final String namespace = tenantToNamespace(tenant);
        final Application application = client.resources(Application.class)
                .inNamespace(namespace)
                .withName(name)
                .get();
        if (application == null) {
            return null;
        }
        return convertApplicationToResult(name, application);
    }

    @Override
    public void delete(String tenant, String name) {
        final String namespace = tenantToNamespace(tenant);

        client.resources(Application.class)
                .inNamespace(namespace)
                .withName(name)
                .delete();
    }

    @Override
    public Map<String, StoredApplicationInstance> list(String tenant) {
        final String namespace = tenantToNamespace(tenant);
        return client.resources(Application.class)
                .inNamespace(namespace)
                .list()
                .getItems().stream()
                .map(a -> convertApplicationToResult(a.getMetadata().getName(), a))
                .collect(Collectors.toMap(StoredApplicationInstance::getName, Function.identity()));
    }

    @SneakyThrows
    private StoredApplicationInstance convertApplicationToResult(String applicationName, Application application) {
        final ApplicationInstance instance =
                mapper.readValue(application.getSpec().getApplication(), SerializedApplicationInstance.class)
                        .toApplicationInstance();

        // TODO: load secrets ?

        final ApplicationStatus status = application.getStatus();
        return StoredApplicationInstance.builder()
                .name(applicationName)
                .instance(instance)
                .status(status == null ? null : status.getStatus())
                .build();
    }


    @Data
    @NoArgsConstructor
    public static class SerializedApplicationInstance {

        public SerializedApplicationInstance(ApplicationInstance applicationInstance) {
            this.resources = applicationInstance.getResources();
            this.modules = applicationInstance.getModules();
            this.instance = applicationInstance.getInstance();
        }

        private Map<String, Resource> resources = new HashMap<>();
        private Map<String, Module> modules = new HashMap<>();
        private Instance instance;

        public ApplicationInstance toApplicationInstance() {
            final ApplicationInstance app = new ApplicationInstance();
            app.setInstance(instance);
            app.setModules(modules);
            app.setResources(resources);
            return app;
        }
    }
}
