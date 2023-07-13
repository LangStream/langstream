package com.datastax.oss.sga.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
public class AppControllerIT {

    @RegisterExtension
    static final OperatorExtension deployment = new OperatorExtension();

    @Test
    void testAppController() throws Exception {

        final ApplicationCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Application
                metadata:
                  name: test-app
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: my-tenant
                """);
        final KubernetesClient client = deployment.getClient();
        final String namespace = "sga-my-tenant";
        client.resource(new NamespaceBuilder()
                .withNewMetadata()
                .withName(namespace)
                .endMetadata().build()).serverSideApply();
        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(1, client.batch().v1().jobs().inNamespace(namespace).list().getItems().size());
        });
        final Job job = client.batch().v1().jobs().inNamespace(namespace).list().getItems().get(0);
        final JobSpec spec = job.getSpec();
        assertEquals(spec.getTemplate().getMetadata().getLabels().get("app"), "sga");
        assertEquals(spec.getTemplate().getMetadata().getLabels().get("tenant"), "my-tenant");

        final PodSpec templateSpec = spec.getTemplate().getSpec();
        final Container container = templateSpec.getContainers().get(0);
        assertEquals("ubuntu", container.getImage());
        assertEquals("Always", container.getImagePullPolicy());
        assertEquals("deployer", container.getName());
        assertEquals(Quantity.parse("100m"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("128Mi"), container.getResources().getRequests().get("memory"));
        assertEquals("/app-config", container.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", container.getVolumeMounts().get(0).getName());
        assertEquals("/app-secrets", container.getVolumeMounts().get(1).getMountPath());
        assertEquals("app-secrets", container.getVolumeMounts().get(1).getName());

        final Container initContainer = templateSpec.getInitContainers().get(0);
        assertEquals("ubuntu", initContainer.getImage());
        assertEquals("Always", initContainer.getImagePullPolicy());
        assertEquals("deployer-init-config", initContainer.getName());
        assertEquals("/app-config", initContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", initContainer.getVolumeMounts().get(0).getName());
        assertEquals("bash", initContainer.getCommand().get(0));
        assertEquals("-c", initContainer.getCommand().get(1));
        assertEquals("echo '{\"application\":\"{app: true}\",\"name\":\"test-app\"}' > /app-config/config", initContainer.getArgs().get(0));
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }

}