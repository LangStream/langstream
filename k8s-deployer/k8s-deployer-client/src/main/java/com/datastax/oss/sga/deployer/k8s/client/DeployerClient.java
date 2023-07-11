package com.datastax.oss.sga.deployer.k8s.client;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.Application;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DeployerClient {

    private final KubernetesClient client;


    public DeployerClient() {
        client = new KubernetesClientBuilder()
                .withConfig(Config.autoConfigure(null))
                .build();
    }


    public void deployApplication(String tenant,
                                  String applicationName, ApplicationInstance applicationInstance) {


        final String namespace = "sga-" + tenant;
        final Application crd = new Application();
        crd.setMetadata(new ObjectMetaBuilder()
                .withName(applicationName)
                .withNamespace(namespace)
                .build());
        final ApplicationSpec spec = ApplicationSpec.builder()
                .image("datastax/sga-runtime:latest")
                .imagePullPolicy("Always")
                .instance(applicationInstance.getInstance())
                .modules(applicationInstance.getModules())
                .resources(applicationInstance.getResources())
                .build();
        crd.setSpec(spec);

        client.resource(crd)
                .inNamespace(namespace)
                .serverSideApply();

    }

    public void deleteApplication(String tenant, String applicationName) {
        final String namespace = "sga-" + tenant;
        client.resources(Application.class)
                .inNamespace(namespace)
                .withName(applicationName)
                .delete();

    }


    public StoredApplicationInstance getApplication(String tenant, String applicationName) {

        final String namespace = "sga-" + tenant;
        final Application application = client.resources(Application.class)
                .inNamespace(namespace)
                .withName(applicationName)
                .get();
        if (application == null) {
            return null;
        }


        return convertApplicationToResult(applicationName, application);
    }

    private StoredApplicationInstance convertApplicationToResult(String applicationName, Application application) {
        final ApplicationInstance instance = new ApplicationInstance();
        instance.setInstance(application.getSpec().getInstance());
        instance.setModules(application.getSpec().getModules());
        instance.setResources(application.getSpec().getResources());

        // TODO: load secrets ?

        return StoredApplicationInstance.builder()
                .name(applicationName)
                .instance(instance)
                // TODO: load the actual status
                .status(ApplicationInstanceLifecycleStatus.DEPLOYED)
                .build();
    }

    public Map<String, StoredApplicationInstance> listApplications(String tenant) {
        final String namespace = "sga-" + tenant;
        return client.resources(Application.class)
                .inNamespace(namespace)
                .list()
                .getItems().stream()
                .map(a -> convertApplicationToResult(a.getMetadata().getName(), a))
                .collect(Collectors.toMap(StoredApplicationInstance::getName, Function.identity()));

    }


}
