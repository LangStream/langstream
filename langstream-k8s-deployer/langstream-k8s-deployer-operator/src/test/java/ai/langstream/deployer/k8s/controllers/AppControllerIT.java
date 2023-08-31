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
package ai.langstream.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
public class AppControllerIT {

    @RegisterExtension
    static final OperatorExtension deployment =
            new OperatorExtension(
                    Map.of(
                            "DEPLOYER_AGENT_RESOURCES", "{defaultMaxUnitsPerTenant: 3}",
                            "DEPLOYER_RUNTIME_IMAGE", "busybox",
                            "DEPLOYER_RUNTIME_IMAGE_PULL_POLICY", "IfNotPresent"));

    @Test
    void testAppController() {

        final String tenant = "my-tenant";
        final String namespace = "langstream-" + tenant;
        final String applicationId = "my-app";
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: %s
                  namespace: %s
                spec:
                    image: busybox
                    imagePullPolicy: IfNotPresent
                    application: "{app: true}"
                    tenant: %s
                """
                                .formatted(applicationId, namespace, tenant));
        final KubernetesClient client = deployment.getClient();
        client.resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(namespace)
                                .endMetadata()
                                .build())
                .serverSideApply();
        client.resource(resource).inNamespace(namespace).create();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            assertEquals(
                                    1,
                                    client.batch()
                                            .v1()
                                            .jobs()
                                            .inNamespace(namespace)
                                            .list()
                                            .getItems()
                                            .size());
                            assertEquals(
                                    ApplicationLifecycleStatus.Status.DEPLOYING,
                                    client.resource(resource)
                                            .inNamespace(namespace)
                                            .get()
                                            .getStatus()
                                            .getStatus()
                                            .getStatus());
                        });
        final Job job = client.batch().v1().jobs().inNamespace(namespace).list().getItems().get(0);
        checkJob(job, false);

        client.resource(resource).inNamespace(namespace).delete();

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertEquals(
                                        2,
                                        client.batch()
                                                .v1()
                                                .jobs()
                                                .inNamespace(namespace)
                                                .list()
                                                .getItems()
                                                .size()));
        final Job cleanupJob =
                client.batch()
                        .v1()
                        .jobs()
                        .inNamespace(namespace)
                        .withName("langstream-runtime-deployer-cleanup-" + applicationId)
                        .get();

        assertNotNull(cleanupJob);
        checkJob(cleanupJob, true);

        // it has to wait for the cleanup job to complete before actually deleting the application
        assertNotNull(client.resource(resource).inNamespace(namespace).get());
    }

    @Test
    void testAppResources() {

        final String tenant = "my-tenant";
        setupTenant(tenant);
        final ApplicationCustomResource app1 = createAppWithResources(tenant, 1, 1);
        awaitApplicationDeployingStatus(app1);

        final ApplicationCustomResource app2 = createAppWithResources(tenant, 1, 1);
        awaitApplicationDeployingStatus(app2);

        final ApplicationCustomResource app3 = createAppWithResources(tenant, 2, 1);
        awaitApplicationErrorForResources(app3);

        deployment.getClient().resource(app2).delete();
        awaitApplicationDeployingStatus(app3);

        final ApplicationCustomResource app4 = createAppWithResources(tenant, 2, 1);
        awaitApplicationErrorForResources(app4);

        deployment.restartDeployerOperator();
        deployment.getClient().resource(app3).delete();
        awaitApplicationDeployingStatus(app4);
    }

    private void awaitApplicationErrorForResources(ApplicationCustomResource original) {
        org.awaitility.Awaitility.await()
                .untilAsserted(
                        () -> {
                            final ApplicationCustomResource resource =
                                    deployment.getClient().resource(original).get();
                            assertEquals(
                                    ApplicationLifecycleStatus.Status.ERROR_DEPLOYING,
                                    resource.getStatus().getStatus().getStatus());
                            assertEquals(
                                    "Not enough resources to deploy application",
                                    resource.getStatus().getStatus().getReason());
                            assertEquals(
                                    ApplicationStatus.ResourceLimitStatus.REJECTED,
                                    resource.getStatus().getResourceLimitStatus());
                        });
    }

    private void awaitApplicationDeployingStatus(ApplicationCustomResource resource) {
        Awaitility.await()
                .atMost(1, java.util.concurrent.TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            final ApplicationCustomResource applicationCustomResource =
                                    deployment
                                            .getClient()
                                            .resource(resource)
                                            .inNamespace(resource.getMetadata().getNamespace())
                                            .get();
                            assertNotNull(applicationCustomResource);
                            assertEquals(
                                    ApplicationLifecycleStatus.Status.DEPLOYING,
                                    applicationCustomResource.getStatus().getStatus().getStatus());
                            assertEquals(
                                    ApplicationStatus.ResourceLimitStatus.ACCEPTED,
                                    applicationCustomResource.getStatus().getResourceLimitStatus());
                        });
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
        assertEquals(
                "/app-config/config",
                container.getEnv().stream()
                        .filter(
                                e ->
                                        "LANGSTREAM_RUNTIME_DEPLOYER_APP_CONFIGURATION"
                                                .equals(e.getName()))
                        .findFirst()
                        .orElseThrow()
                        .getValue());
        assertEquals(
                "/cluster-runtime-config/config",
                container.getEnv().stream()
                        .filter(
                                e ->
                                        "LANGSTREAM_RUNTIME_DEPLOYER_CLUSTER_RUNTIME_CONFIGURATION"
                                                .equals(e.getName()))
                        .findFirst()
                        .orElseThrow()
                        .getValue());
        assertEquals(
                "/app-secrets/secrets",
                container.getEnv().stream()
                        .filter(e -> "LANGSTREAM_RUNTIME_DEPLOYER_APP_SECRETS".equals(e.getName()))
                        .findFirst()
                        .orElseThrow()
                        .getValue());

        final Container initContainer = templateSpec.getInitContainers().get(0);
        assertEquals("busybox", initContainer.getImage());
        assertEquals("IfNotPresent", initContainer.getImagePullPolicy());
        assertEquals("deployer-init-config", initContainer.getName());
        assertEquals("/app-config", initContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", initContainer.getVolumeMounts().get(0).getName());
        assertEquals(
                "/cluster-runtime-config", initContainer.getVolumeMounts().get(1).getMountPath());
        assertEquals("cluster-runtime-config", initContainer.getVolumeMounts().get(1).getName());
        assertEquals("bash", initContainer.getCommand().get(0));
        assertEquals("-c", initContainer.getCommand().get(1));
        assertEquals(
                "echo '{\"applicationId\":\"my-app\",\"tenant\":\"my-tenant\",\"application\":\"{app: true}\","
                        + "\"codeStorageArchiveId\":null}' > /app-config/config && echo '{}' > /cluster-runtime-config/config",
                initContainer.getArgs().get(0));
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }

    private void setupTenant(String tenant) {
        deployment
                .getClient()
                .resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName("langstream-" + tenant)
                                .endMetadata()
                                .build())
                .create();
    }

    private ApplicationCustomResource createAppWithResources(
            String tenant, int size, int parallelism) {
        final String appId = genAppId();
        ApplicationCustomResource resource =
                getCr(
                        """
                                apiVersion: langstream.ai/v1alpha1
                                kind: Application
                                metadata:
                                  name: %s
                                spec:
                                    application: '{"agentRunners": {"agent1": {"resources": {"size": %d, "parallelism": %d}}}}'
                                    tenant: %s
                                """
                                .formatted(appId, size, parallelism, tenant));
        return deployment
                .getClient()
                .resource(resource)
                .inNamespace("langstream-" + tenant)
                .create();
    }

    AtomicInteger counter = new AtomicInteger(0);

    private String genAppId() {
        return "app-%s".formatted(counter.incrementAndGet());
    }
}
