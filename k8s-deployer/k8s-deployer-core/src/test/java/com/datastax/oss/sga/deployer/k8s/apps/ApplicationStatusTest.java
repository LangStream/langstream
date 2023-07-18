package com.datastax.oss.sga.deployer.k8s.apps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.api.model.ApplicationLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationStatus;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ApplicationStatusTest {


    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testApplicationStatus() {
        final String tenant = "my-tenant";
        final String namespace = "sga-%s".formatted(tenant);
        final String applicationId = "my-app";
        final ApplicationCustomResource cr =
                deployApp(tenant, namespace, applicationId);
        Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {

            final ApplicationLifecycleStatus status =
                    AppResourcesFactory.computeApplicationStatus(k3s.getClient(), cr);
            System.out.println("got status" + status);
            assertEquals(ApplicationLifecycleStatus.Status.ERROR_DEPLOYING, status.getStatus());
            assertEquals("failed to create containerd task: failed to create shim task: OCI runtime create failed: "
                    + "runc create failed: unable to start container process: exec: \"agent-runtime\": executable "
                    + "file not found in $PATH: unknown", status.getReason());

        });
    }

    private ApplicationCustomResource deployApp(String tenant, String namespace, String applicationId) {
        k3s.getClient().resource(new NamespaceBuilder()
                        .withNewMetadata().withName(namespace).endMetadata().build())
                .serverSideApply();
        k3s.getClient().resource(new ServiceAccountBuilder()
                        .withNewMetadata().withName(tenant).endMetadata().build())
                .inNamespace(namespace)
                .serverSideApply();
        final ApplicationCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Application
                metadata:
                  name: %s
                  namespace: %s
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: %s
                """.formatted(applicationId, namespace, tenant));
        resource.getMetadata().setLabels(AppResourcesFactory.getLabels(false, applicationId));
        final ApplicationStatus status = new ApplicationStatus();
        status.setStatus(ApplicationLifecycleStatus.DEPLOYING);
        resource.setStatus(status);
        k3s.getClient().resource(resource).inNamespace(namespace).serverSideApply();
        final Job jo = AppResourcesFactory.generateJob(resource, Map.of(), false);
        k3s.getClient().resource(jo).inNamespace(namespace).serverSideApply();
        return resource;
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }
}