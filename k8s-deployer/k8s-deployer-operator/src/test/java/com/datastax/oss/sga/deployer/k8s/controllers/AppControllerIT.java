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
package com.datastax.oss.sga.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import com.datastax.oss.sga.api.model.ApplicationLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
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

        final String tenant = "my-tenant";
        final String namespace = "sga-" + tenant;
        final String applicationId = "my-app";
        final ApplicationCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Application
                metadata:
                  name: %s
                  namespace: %s
                spec:
                    image: busybox
                    imagePullPolicy: IfNotPresent
                    application: "{app: true}"
                    tenant: %s
                """.formatted(applicationId, namespace, tenant));
        final KubernetesClient client = deployment.getClient();
        client.resource(new NamespaceBuilder()
                .withNewMetadata()
                .withName(namespace)
                .endMetadata().build()).serverSideApply();
        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(1, client.batch().v1().jobs().inNamespace(namespace).list().getItems().size());
            assertEquals(ApplicationLifecycleStatus.Status.DEPLOYING,
                    client.resource(resource).inNamespace(namespace).get().getStatus().getStatus().getStatus());
        });
        final Job job = client.batch().v1().jobs().inNamespace(namespace).list().getItems().get(0);
        checkJob(job, false);


        client.resource(resource).inNamespace(namespace).delete();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(2, client.batch().v1().jobs().inNamespace(namespace).list().getItems().size());
        });
        final Job cleanupJob =
                client.batch().v1().jobs().inNamespace(namespace).withName("sga-runtime-deployer-cleanup-" + applicationId)
                        .get();

        assertNotNull(cleanupJob);
        checkJob(cleanupJob, true);

        // it has to wait for the cleanup job to complete before actually deleting the application
        assertNotNull(client.resource(resource).inNamespace(namespace).get());
    }

    private void checkJob(Job job, boolean cleanup) {
        final JobSpec spec = job.getSpec();
        final PodSpec templateSpec = spec.getTemplate().getSpec();
        final Container container = templateSpec.getContainers().get(0);
        assertEquals("busybox", container.getImage());
        assertEquals("IfNotPresent", container.getImagePullPolicy());
        assertEquals("deployer", container.getName());
        assertEquals(Quantity.parse("100m"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("128Mi"), container.getResources().getRequests().get("memory"));
        assertEquals("/app-config", container.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", container.getVolumeMounts().get(0).getName());
        assertEquals("/app-secrets", container.getVolumeMounts().get(1).getMountPath());
        assertEquals("app-secrets", container.getVolumeMounts().get(1).getName());
        assertEquals(0, container.getCommand().size());
        if (cleanup) {
            int args = 0;
            assertEquals("deployer-runtime", container.getArgs().get(args++));
            assertEquals("delete", container.getArgs().get(args++));
            assertEquals("/cluster-runtime-config/config", container.getArgs().get(args++));
            assertEquals("/app-config/config", container.getArgs().get(args++));
            assertEquals("/app-secrets/secrets", container.getArgs().get(args++));
        } else {
            int args = 0;
            assertEquals("deployer-runtime", container.getArgs().get(args++));
            assertEquals("deploy", container.getArgs().get(args++));
            assertEquals("/cluster-runtime-config/config", container.getArgs().get(args++));
            assertEquals("/app-config/config", container.getArgs().get(args++));
            assertEquals("/app-secrets/secrets", container.getArgs().get(args++));
        }

        final Container initContainer = templateSpec.getInitContainers().get(0);
        assertEquals("busybox", initContainer.getImage());
        assertEquals("IfNotPresent", initContainer.getImagePullPolicy());
        assertEquals("deployer-init-config", initContainer.getName());
        assertEquals("/app-config", initContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", initContainer.getVolumeMounts().get(0).getName());
        assertEquals("/cluster-runtime-config", initContainer.getVolumeMounts().get(1).getMountPath());
        assertEquals("cluster-runtime-config", initContainer.getVolumeMounts().get(1).getName());
        assertEquals("bash", initContainer.getCommand().get(0));
        assertEquals("-c", initContainer.getCommand().get(1));
        assertEquals("echo '{\"applicationId\":\"my-app\",\"tenant\":\"my-tenant\",\"application\":\"{app: true}\","
                + "\"codeStorageArchiveId\":null}' > /app-config/config && echo '{}' > /cluster-runtime-config/config", initContainer.getArgs().get(0));
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }

}
