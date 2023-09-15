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
package ai.langstream.deployer.k8s.apps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ApplicationCustomResourceTest {

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testApplicationStatus() {
        final String tenant = genTenant();
        final String applicationId = "my-app";
        final ApplicationCustomResource cr = deployApp(tenant, applicationId, false);
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            final ApplicationLifecycleStatus status =
                                    AppResourcesFactory.computeApplicationStatus(
                                            k3s.getClient(), k3s.getClient().resource(cr).get());
                            System.out.println("got status" + status);
                            assertEquals(
                                    ApplicationLifecycleStatus.Status.ERROR_DEPLOYING,
                                    status.getStatus());
                            assertEquals(
                                    "failed to create containerd task: failed to create shim task: OCI runtime create failed: "
                                            + "runc create failed: unable to start container process: exec: \"bash\": executable file not "
                                            + "found in $PATH: unknown",
                                    status.getReason());
                        });
    }

    @Test
    void testApplicationStatusDeleting() {
        final String tenant = genTenant();
        final String applicationId = "my-app";
        final ApplicationCustomResource cr = deployApp(tenant, applicationId, true);
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            final ApplicationLifecycleStatus status =
                                    AppResourcesFactory.computeApplicationStatus(
                                            k3s.getClient(), k3s.getClient().resource(cr).get());
                            System.out.println("got status" + status);
                            assertEquals(
                                    ApplicationLifecycleStatus.Status.ERROR_DELETING,
                                    status.getStatus());
                            assertEquals(
                                    "failed to create containerd task: failed to create shim task: OCI runtime create failed: "
                                            + "runc create failed: unable to start container process: exec: \"bash\": executable file not "
                                            + "found in $PATH: unknown",
                                    status.getReason());
                        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testJobsBeingDeleted(boolean isDeleteJob) {
        final String tenant = genTenant();
        final String namespace = "langstream-%s".formatted(tenant);
        final String applicationId = "my-app";
        final ApplicationCustomResource cr = deployApp(tenant, applicationId, isDeleteJob);
        assertEquals(
                2,
                k3s.getClient()
                        .batch()
                        .v1()
                        .jobs()
                        .inNamespace(namespace)
                        .list()
                        .getItems()
                        .size());

        k3s.getClient().resource(cr).delete();

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertEquals(
                                        0,
                                        k3s.getClient()
                                                .batch()
                                                .v1()
                                                .jobs()
                                                .inNamespace(namespace)
                                                .list()
                                                .getItems()
                                                .size()));
    }

    static AtomicInteger counter = new AtomicInteger(0);

    private static String genTenant() {
        return "tenant-" + counter.incrementAndGet();
    }

    private ApplicationCustomResource deployApp(
            String tenant, String applicationId, boolean deleteJob) {
        final String namespace = "langstream-%s".formatted(tenant);
        final Namespace namespaceResource =
                new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build();
        k3s.getClient().resource(namespaceResource).serverSideApply();
        k3s.getClient()
                .resource(
                        new ServiceAccountBuilder()
                                .withNewMetadata()
                                .withName(tenant)
                                .endMetadata()
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
        k3s.getClient()
                .resource(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(applicationId)
                                .endMetadata()
                                .withData(
                                        Map.of(
                                                "secrets",
                                                Base64.getEncoder()
                                                        .encodeToString(
                                                                "{}"
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8))))
                                .build())
                .inNamespace(namespace)
                .serverSideApply();

        k3s.getClient()
                .resource(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                                .endMetadata()
                                .withData(
                                        Map.of(
                                                CRDConstants.TENANT_CLUSTER_CONFIG_SECRET_KEY,
                                                Base64.getEncoder()
                                                        .encodeToString(
                                                                "{}"
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8))))
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
        ApplicationCustomResource resource =
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
        resource.getMetadata()
                .setLabels(AppResourcesFactory.getLabelsForDeployer(deleteJob, applicationId));

        k3s.getClient().resource(resource).inNamespace(namespace).serverSideApply();
        resource = k3s.getClient().resource(resource).get();
        final ApplicationStatus status = new ApplicationStatus();
        if (deleteJob) {
            status.setStatus(ApplicationLifecycleStatus.DELETING);
        } else {
            status.setStatus(ApplicationLifecycleStatus.DEPLOYING);
        }
        resource.setStatus(status);
        k3s.getClient().resource(resource).inNamespace(namespace).updateStatus();
        k3s.getClient()
                .resource(
                        AppResourcesFactory.generateDeployerJob(
                                AppResourcesFactory.GenerateJobParams.builder()
                                        .applicationCustomResource(resource)
                                        .deleteJob(deleteJob)
                                        .build()))
                .inNamespace(namespace)
                .serverSideApply();

        k3s.getClient()
                .resource(
                        AppResourcesFactory.generateSetupJob(
                                AppResourcesFactory.GenerateJobParams.builder()
                                        .applicationCustomResource(resource)
                                        .deleteJob(deleteJob)
                                        .build()))
                .inNamespace(namespace)
                .serverSideApply();
        return resource;
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }
}
