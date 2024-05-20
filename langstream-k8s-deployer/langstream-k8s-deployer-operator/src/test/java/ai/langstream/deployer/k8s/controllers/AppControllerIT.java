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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.controllers.apps.AppController;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
                            "DEPLOYER_AGENT_RESOURCES",
                            "{defaultMaxTotalResourceUnitsPerTenant: 3}",
                            "DEPLOYER_RUNTIME_IMAGE",
                            "bash",
                            "DEPLOYER_RUNTIME_IMAGE_PULL_POLICY",
                            "IfNotPresent"));

    static AtomicInteger counter = new AtomicInteger(0);

    static String genTenant() {
        return "tenant-" + counter.incrementAndGet();
    }

    @Test
    void testAppController() {

        final String tenant = "my-tenant";
        setupTenant(tenant);
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
                                    image: bash
                                    imagePullPolicy: IfNotPresent
                                    application: '{"modules": {}}'
                                    tenant: %s
                                """
                                .formatted(applicationId, namespace, tenant));
        final KubernetesClient client = deployment.getClient();
        deployment
                .getClient()
                .resource(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(applicationId)
                                .endMetadata()
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
        final ApplicationCustomResource createdCr =
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
        Job job = client.batch().v1().jobs().inNamespace(namespace).list().getItems().get(0);
        checkSetupJob(job);
        // simulate job finished
        client.resource(job).inNamespace(namespace).delete();

        createMockJob(namespace, client, job.getMetadata().getName());
        patchCustomResourceWithStatusDone(createdCr);
        awaitJobCompleted(namespace, job.getMetadata().getName());

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () -> {
                            assertNotNull(
                                    client.batch()
                                            .v1()
                                            .jobs()
                                            .inNamespace(namespace)
                                            .withName(
                                                    AppResourcesFactory.getDeployerJobName(
                                                            applicationId, false))
                                            .get());
                            assertEquals(
                                    ApplicationLifecycleStatus.Status.DEPLOYING,
                                    client.resource(resource)
                                            .inNamespace(namespace)
                                            .get()
                                            .getStatus()
                                            .getStatus()
                                            .getStatus());
                        });
        job =
                client.batch()
                        .v1()
                        .jobs()
                        .inNamespace(namespace)
                        .withName(AppResourcesFactory.getDeployerJobName(applicationId, false))
                        .get();

        checkDeployerJob(job, false);

        client.resource(resource).inNamespace(namespace).delete();

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertEquals(
                                        3,
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
        checkDeployerJob(cleanupJob, true);

        // it has to wait for the cleanup job to complete before actually deleting the application
        assertNotNull(client.resource(resource).inNamespace(namespace).get());
    }

    private void createMockJob(String namespace, KubernetesClient client, String name) {
        final Job mockJob =
                new JobBuilder()
                        .withNewMetadata()
                        .withName(name)
                        .endMetadata()
                        .withNewSpec()
                        .withNewTemplate()
                        .withNewSpec()
                        .withContainers(
                                List.of(
                                        new ContainerBuilder()
                                                .withName("test")
                                                .withImage("bash")
                                                .withCommand(List.of("sleep", "1"))
                                                .build()))
                        .withRestartPolicy("Never")
                        .endSpec()
                        .endTemplate()
                        .endSpec()
                        .build();
        client.resource(mockJob).inNamespace(namespace).create();
    }

    @Test
    void testAppResources() {

        final String tenant = genTenant();
        setupTenant(tenant);
        final ApplicationCustomResource app1 = createAppWithResources(tenant, 1, 1);
        awaitApplicationDeployingStatus(app1);

        final ApplicationCustomResource app2 = createAppWithResources(tenant, 1, 1);
        awaitApplicationDeployingStatus(app2);

        final ApplicationCustomResource app3 = createAppWithResources(tenant, 2, 1);
        awaitApplicationErrorForResources(app3);

        simulateAppDeletion(app2);
        awaitApplicationDeployingStatus(app3);

        final ApplicationCustomResource app4 = createAppWithResources(tenant, 2, 1);
        awaitApplicationErrorForResources(app4);

        deployment.restartDeployerOperator();
        simulateAppDeletion(app3);
        awaitApplicationDeployingStatus(app4);
    }

    private void simulateAppDeletion(ApplicationCustomResource app) {
        final String namespace = app.getMetadata().getNamespace();
        final String deployerJobName =
                AppResourcesFactory.getDeployerJobName(app.getMetadata().getName(), true);
        final String setupJobName =
                AppResourcesFactory.getSetupJobName(app.getMetadata().getName(), true);
        createMockJob(namespace, deployment.getClient(), setupJobName);

        createMockJob(namespace, deployment.getClient(), deployerJobName);

        awaitJobCompleted(namespace, deployerJobName);
        awaitJobCompleted(namespace, setupJobName);
        patchCustomResourceWithStatusDone(app);

        deployment
                .getClient()
                .resources(ApplicationCustomResource.class)
                .inNamespace(app.getMetadata().getNamespace())
                .withName(app.getMetadata().getName())
                .delete();
    }

    private void patchCustomResourceWithStatusDone(ApplicationCustomResource app) {
        final ApplicationStatus status = new ApplicationStatus();
        final AppController.AppLastApplied appLastApplied = new AppController.AppLastApplied();
        appLastApplied.setSetup(SerializationUtil.writeAsJson(app.getSpec()));
        appLastApplied.setRuntimeDeployer(SerializationUtil.writeAsJson(app.getSpec()));
        status.setLastApplied(SerializationUtil.writeAsJson(appLastApplied));
        final ApplicationCustomResource resource =
                deployment
                        .getClient()
                        .resources(ApplicationCustomResource.class)
                        .inNamespace(app.getMetadata().getNamespace())
                        .withName(app.getMetadata().getName())
                        .get();
        resource.setStatus(status);
        deployment.getClient().resource(resource).patchStatus();
    }

    private void awaitJobCompleted(String namespace, String deployerJobName) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            final Integer succeeded =
                                    deployment
                                            .getClient()
                                            .batch()
                                            .v1()
                                            .jobs()
                                            .inNamespace(namespace)
                                            .withName(deployerJobName)
                                            .get()
                                            .getStatus()
                                            .getSucceeded();
                            assertTrue(succeeded != null && succeeded > 0);
                        });
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

    private void checkSetupJob(Job job) {
        final JobSpec spec = job.getSpec();
        final PodSpec templateSpec = spec.getTemplate().getSpec();
        final Container container = templateSpec.getContainers().get(0);
        assertEquals("bash", container.getImage());
        assertEquals("IfNotPresent", container.getImagePullPolicy());
        assertEquals("setup", container.getName());
        assertEquals(Quantity.parse("100m"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("128Mi"), container.getResources().getRequests().get("memory"));
        assertEquals("/app-config", container.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", container.getVolumeMounts().get(0).getName());
        assertEquals("/app-secrets", container.getVolumeMounts().get(1).getMountPath());
        assertEquals("app-secrets", container.getVolumeMounts().get(1).getName());
        assertEquals(0, container.getCommand().size());
        int args = 0;
        assertEquals("application-setup", container.getArgs().get(args++));
        assertEquals("deploy", container.getArgs().get(args++));

        final Container initContainer = templateSpec.getInitContainers().get(0);
        assertEquals("bash", initContainer.getImage());
        assertEquals("IfNotPresent", initContainer.getImagePullPolicy());
        assertEquals("setup-init-config", initContainer.getName());
        assertEquals("/app-config", initContainer.getVolumeMounts().get(0).getMountPath());
        assertEquals("app-config", initContainer.getVolumeMounts().get(0).getName());
        assertEquals(
                "/cluster-runtime-config", initContainer.getVolumeMounts().get(1).getMountPath());
        assertEquals("cluster-runtime-config", initContainer.getVolumeMounts().get(1).getName());
        assertEquals("bash", initContainer.getCommand().get(0));
        assertEquals("-c", initContainer.getCommand().get(1));
        assertEquals(
                "echo '{\"applicationId\":\"my-app\",\"tenant\":\"my-tenant\",\"application\":\"{\\\"modules\\\": "
                        + "{}}\",\"codeArchiveId\":null}' > /app-config/config && echo '{}' > /cluster-runtime-config/config",
                initContainer.getArgs().get(0));
    }

    private void checkDeployerJob(Job job, boolean cleanup) {
        final JobSpec spec = job.getSpec();
        final PodSpec templateSpec = spec.getTemplate().getSpec();
        final Container container = templateSpec.getContainers().get(0);
        assertEquals("bash", container.getImage());
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
        assertEquals("bash", initContainer.getImage());
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
                "echo '{\"applicationId\":\"my-app\",\"tenant\":\"my-tenant\",\"application\":\"{\\\"modules\\\": {}}\",\"codeStorageArchiveId\":null,\"deployFlags\":{\"autoUpgradeRuntimeImage\":false,\"autoUpgradeRuntimeImagePullPolicy\":false,\"autoUpgradeAgentResources\":false,\"autoUpgradeAgentPodTemplate\":false,\"seed\":0}}' > /app-config/config && echo '{}' > /cluster-runtime-config/config",
                initContainer.getArgs().get(0));
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }

    private void setupTenant(String tenant) {
        final String namespace = "langstream-" + tenant;
        deployment
                .getClient()
                .resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(namespace)
                                .endMetadata()
                                .build())
                .create();
        deployment
                .getClient()
                .resource(
                        new ServiceAccountBuilder()
                                .withNewMetadata()
                                .withName(
                                        CRDConstants.computeRuntimeServiceAccountForTenant(tenant))
                                .endMetadata()
                                .build())
                .inNamespace(namespace)
                .serverSideApply();

        deployment
                .getClient()
                .resource(
                        new ServiceAccountBuilder()
                                .withNewMetadata()
                                .withName(
                                        CRDConstants.computeDeployerServiceAccountForTenant(tenant))
                                .endMetadata()
                                .build())
                .inNamespace(namespace)
                .serverSideApply();

        deployment
                .getClient()
                .resource(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                                .endMetadata()
                                .withData(Map.of(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET_KEY, ""))
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
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
        final String namespace = "langstream-" + tenant;
        deployment
                .getClient()
                .resource(
                        new SecretBuilder().withNewMetadata().withName(appId).endMetadata().build())
                .inNamespace(namespace)
                .serverSideApply();
        return deployment.getClient().resource(resource).inNamespace(namespace).create();
    }

    private String genAppId() {
        return "app-%s".formatted(counter.incrementAndGet());
    }
}
