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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.deployer.k8s.PodTemplate;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AppResourcesFactoryTest {

    @Test
    void testDeployerJob() {
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: test-'app
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                """);

        assertEquals(
                """
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: langstream-deployer
                            langstream-application: test-'app
                            langstream-scope: deploy
                          name: langstream-runtime-deployer-test-'app
                          namespace: default
                          ownerReferences:
                          - apiVersion: langstream.ai/v1alpha1
                            kind: Application
                            blockOwnerDeletion: true
                            controller: true
                            name: test-'app
                        spec:
                          backoffLimit: 0
                          template:
                            metadata:
                              labels:
                                app: langstream-deployer
                                langstream-application: test-'app
                                langstream-scope: deploy
                            spec:
                              containers:
                              - args:
                                - deployer-runtime
                                - deploy
                                - /cluster-runtime-config/config
                                - /app-config/config
                                - /app-secrets/secrets
                                env:
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_APP_CONFIGURATION
                                  value: /app-config/config
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_CLUSTER_RUNTIME_CONFIGURATION
                                  value: /cluster-runtime-config/config
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_APP_SECRETS
                                  value: /app-secrets/secrets
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_CLUSTER_CONFIGURATION
                                  value: /cluster-config/config
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_TOKEN
                                  value: /var/run/secrets/kubernetes.io/serviceaccount/token
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 128Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-secrets
                                  name: app-secrets
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                                - mountPath: /cluster-config
                                  name: cluster-config
                              initContainers:
                              - args:
                                - "echo '{\\"applicationId\\":\\"test-'\\"'\\"'app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\",\\"codeStorageArchiveId\\":\\"iiii\\",\\"deployFlags\\":{\\"autoUpgradeRuntimeImage\\":false,\\"autoUpgradeRuntimeImagePullPolicy\\":false,\\"autoUpgradeAgentResources\\":false,\\"autoUpgradeAgentPodTemplate\\":false,\\"seed\\":0}}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
                                command:
                                - bash
                                - -c
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer-init-config
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              restartPolicy: Never
                              serviceAccountName: my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-'app
                              - emptyDir: {}
                                name: cluster-runtime-config
                              - name: cluster-config
                                secret:
                                  items:
                                  - key: cluster-config
                                    path: config
                                  secretName: langstream-cluster-config
                        """,
                SerializationUtil.writeAsYaml(
                        AppResourcesFactory.generateDeployerJob(
                                AppResourcesFactory.GenerateJobParams.builder()
                                        .applicationCustomResource(resource)
                                        .build())));

        assertEquals(
                """
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: langstream-deployer
                            langstream-application: test-'app
                            langstream-scope: delete
                          name: langstream-runtime-deployer-cleanup-test-'app
                          namespace: default
                          ownerReferences:
                          - apiVersion: langstream.ai/v1alpha1
                            kind: Application
                            blockOwnerDeletion: true
                            controller: true
                            name: test-'app
                        spec:
                          backoffLimit: 0
                          template:
                            metadata:
                              labels:
                                app: langstream-deployer
                                langstream-application: test-'app
                                langstream-scope: delete
                            spec:
                              containers:
                              - args:
                                - deployer-runtime
                                - delete
                                - /cluster-runtime-config/config
                                - /app-config/config
                                - /app-secrets/secrets
                                env:
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_APP_CONFIGURATION
                                  value: /app-config/config
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_CLUSTER_RUNTIME_CONFIGURATION
                                  value: /cluster-runtime-config/config
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_APP_SECRETS
                                  value: /app-secrets/secrets
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_CLUSTER_CONFIGURATION
                                  value: /cluster-config/config
                                - name: LANGSTREAM_RUNTIME_DEPLOYER_TOKEN
                                  value: /var/run/secrets/kubernetes.io/serviceaccount/token
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 128Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-secrets
                                  name: app-secrets
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                                - mountPath: /cluster-config
                                  name: cluster-config
                              initContainers:
                              - args:
                                - "echo '{\\"applicationId\\":\\"test-'\\"'\\"'app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\",\\"codeStorageArchiveId\\":\\"iiii\\",\\"deployFlags\\":{\\"autoUpgradeRuntimeImage\\":false,\\"autoUpgradeRuntimeImagePullPolicy\\":false,\\"autoUpgradeAgentResources\\":false,\\"autoUpgradeAgentPodTemplate\\":false,\\"seed\\":0}}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
                                command:
                                - bash
                                - -c
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer-init-config
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              restartPolicy: Never
                              serviceAccountName: my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-'app
                              - emptyDir: {}
                                name: cluster-runtime-config
                              - name: cluster-config
                                secret:
                                  items:
                                  - key: cluster-config
                                    path: config
                                  secretName: langstream-cluster-config
                        """,
                SerializationUtil.writeAsYaml(
                        AppResourcesFactory.generateDeployerJob(
                                AppResourcesFactory.GenerateJobParams.builder()
                                        .applicationCustomResource(resource)
                                        .deleteJob(true)
                                        .build())));
    }

    @Test
    void testSetupJob() {
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: test-'app
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                """);

        assertEquals(
                """
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: langstream-setup
                            langstream-application: test-'app
                            langstream-scope: deploy
                          name: langstream-app-setup-test-'app
                          namespace: default
                          ownerReferences:
                          - apiVersion: langstream.ai/v1alpha1
                            kind: Application
                            blockOwnerDeletion: true
                            controller: true
                            name: test-'app
                        spec:
                          backoffLimit: 0
                          template:
                            metadata:
                              labels:
                                app: langstream-setup
                                langstream-application: test-'app
                                langstream-scope: deploy
                            spec:
                              containers:
                              - args:
                                - application-setup
                                - deploy
                                env:
                                - name: LANGSTREAM_APPLICATION_SETUP_APP_CONFIGURATION
                                  value: /app-config/config
                                - name: LANGSTREAM_APPLICATION_SETUP_CLUSTER_RUNTIME_CONFIGURATION
                                  value: /cluster-runtime-config/config
                                - name: LANGSTREAM_APPLICATION_SETUP_APP_SECRETS
                                  value: /app-secrets/secrets
                                - name: LANGSTREAM_APPLICATION_SETUP_CLUSTER_CONFIGURATION
                                  value: /cluster-config/config
                                - name: LANGSTREAM_APPLICATION_SETUP_TOKEN
                                  value: /var/run/secrets/kubernetes.io/serviceaccount/token
                                image: ubuntu
                                imagePullPolicy: Always
                                name: setup
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 128Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-secrets
                                  name: app-secrets
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                                - mountPath: /cluster-config
                                  name: cluster-config
                              initContainers:
                              - args:
                                - "echo '{\\"applicationId\\":\\"test-'\\"'\\"'app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\",\\"codeArchiveId\\":\\"iiii\\"}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
                                command:
                                - bash
                                - -c
                                image: ubuntu
                                imagePullPolicy: Always
                                name: setup-init-config
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              restartPolicy: Never
                              serviceAccountName: runtime-my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-'app
                              - emptyDir: {}
                                name: cluster-runtime-config
                              - name: cluster-config
                                secret:
                                  items:
                                  - key: cluster-config
                                    path: config
                                  secretName: langstream-cluster-config
                        """,
                SerializationUtil.writeAsYaml(
                        AppResourcesFactory.generateSetupJob(
                                AppResourcesFactory.GenerateJobParams.builder()
                                        .applicationCustomResource(resource)
                                        .build())));

        assertEquals(
                """
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: langstream-setup
                            langstream-application: test-'app
                            langstream-scope: delete
                          name: langstream-app-setup-cleanup-test-'app
                          namespace: default
                          ownerReferences:
                          - apiVersion: langstream.ai/v1alpha1
                            kind: Application
                            blockOwnerDeletion: true
                            controller: true
                            name: test-'app
                        spec:
                          backoffLimit: 0
                          template:
                            metadata:
                              labels:
                                app: langstream-setup
                                langstream-application: test-'app
                                langstream-scope: delete
                            spec:
                              containers:
                              - args:
                                - application-setup
                                - cleanup
                                env:
                                - name: LANGSTREAM_APPLICATION_SETUP_APP_CONFIGURATION
                                  value: /app-config/config
                                - name: LANGSTREAM_APPLICATION_SETUP_CLUSTER_RUNTIME_CONFIGURATION
                                  value: /cluster-runtime-config/config
                                - name: LANGSTREAM_APPLICATION_SETUP_APP_SECRETS
                                  value: /app-secrets/secrets
                                - name: LANGSTREAM_APPLICATION_SETUP_CLUSTER_CONFIGURATION
                                  value: /cluster-config/config
                                - name: LANGSTREAM_APPLICATION_SETUP_TOKEN
                                  value: /var/run/secrets/kubernetes.io/serviceaccount/token
                                image: ubuntu
                                imagePullPolicy: Always
                                name: setup
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 128Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-secrets
                                  name: app-secrets
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                                - mountPath: /cluster-config
                                  name: cluster-config
                              initContainers:
                              - args:
                                - "echo '{\\"applicationId\\":\\"test-'\\"'\\"'app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\",\\"codeArchiveId\\":\\"iiii\\"}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
                                command:
                                - bash
                                - -c
                                image: ubuntu
                                imagePullPolicy: Always
                                name: setup-init-config
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              restartPolicy: Never
                              serviceAccountName: runtime-my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-'app
                              - emptyDir: {}
                                name: cluster-runtime-config
                              - name: cluster-config
                                secret:
                                  items:
                                  - key: cluster-config
                                    path: config
                                  secretName: langstream-cluster-config
                        """,
                SerializationUtil.writeAsYaml(
                        AppResourcesFactory.generateSetupJob(
                                AppResourcesFactory.GenerateJobParams.builder()
                                        .applicationCustomResource(resource)
                                        .deleteJob(true)
                                        .build())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPodTemplate(boolean deleteJob) {
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: test-'app
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                """);
        final PodTemplate podTemplate =
                new PodTemplate(
                        List.of(
                                new TolerationBuilder()
                                        .withEffect("NoSchedule")
                                        .withValue("langstream")
                                        .withKey("workload")
                                        .build()),
                        Map.of("workload", "langstream"),
                        Map.of("ann1", "value1"));

        Job job =
                AppResourcesFactory.generateDeployerJob(
                        AppResourcesFactory.GenerateJobParams.builder()
                                .applicationCustomResource(resource)
                                .deleteJob(deleteJob)
                                .podTemplate(podTemplate)
                                .build());
        final List<Toleration> tolerations = job.getSpec().getTemplate().getSpec().getTolerations();
        assertEquals(1, tolerations.size());
        final Toleration tol = tolerations.get(0);
        assertEquals("workload", tol.getKey());
        assertEquals("langstream", tol.getValue());
        assertEquals("NoSchedule", tol.getEffect());
        assertEquals(
                Map.of("workload", "langstream"),
                job.getSpec().getTemplate().getSpec().getNodeSelector());
        assertEquals(
                "value1", job.getSpec().getTemplate().getMetadata().getAnnotations().get("ann1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSetImage(boolean deleteJob) {
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: test-'app
                  namespace: default
                spec:
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                """);

        Job job =
                AppResourcesFactory.generateDeployerJob(
                        AppResourcesFactory.GenerateJobParams.builder()
                                .applicationCustomResource(resource)
                                .deleteJob(deleteJob)
                                .image("busybox:v1")
                                .imagePullPolicy("Never")
                                .build());
        final Container container = job.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals("busybox:v1", container.getImage());
        assertEquals("Never", container.getImagePullPolicy());
    }

    @Test
    void testNoUpdateFlags() {
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: test-'app
                  namespace: default
                spec:
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                    options: '{"autoUpgradeRuntimeImage": false, "autoUpgradeRuntimeImagePullPolicy": false, "autoUpgradeAgentResources": false, "autoUpgradeAgentPodTemplate": false}'
                """);

        Job job =
                AppResourcesFactory.generateDeployerJob(
                        AppResourcesFactory.GenerateJobParams.builder()
                                .applicationCustomResource(resource)
                                .deleteJob(false)
                                .image("busybox:v1")
                                .imagePullPolicy("Never")
                                .build());
        final Container container =
                job.getSpec().getTemplate().getSpec().getInitContainers().get(0);
        System.out.println("args=" + container.getArgs().get(0));
        assertTrue(
                container
                        .getArgs()
                        .get(0)
                        .contains(
                                "\"deployFlags\":{\"autoUpgradeRuntimeImage\":false,\"autoUpgradeRuntimeImagePullPolicy\":false,\"autoUpgradeAgentResources\":false,\"autoUpgradeAgentPodTemplate\":false,\"seed\":0}"));
    }

    @Test
    void testUpdateFlags() {
        final ApplicationCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Application
                metadata:
                  name: test-'app
                  namespace: default
                spec:
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                    options: '{"autoUpgradeRuntimeImage": true, "autoUpgradeRuntimeImagePullPolicy": true, "autoUpgradeAgentResources": true, "autoUpgradeAgentPodTemplate": true}'
                """);

        Job job =
                AppResourcesFactory.generateDeployerJob(
                        AppResourcesFactory.GenerateJobParams.builder()
                                .applicationCustomResource(resource)
                                .deleteJob(false)
                                .image("busybox:v1")
                                .imagePullPolicy("Never")
                                .build());
        final Container container =
                job.getSpec().getTemplate().getSpec().getInitContainers().get(0);
        System.out.println("args=" + container.getArgs().get(0));
        assertTrue(
                container
                        .getArgs()
                        .get(0)
                        .contains(
                                "\"deployFlags\":{\"autoUpgradeRuntimeImage\":true,\"autoUpgradeRuntimeImagePullPolicy\":true,\"autoUpgradeAgentResources\":true,\"autoUpgradeAgentPodTemplate\":true,\"seed\":0}"));
    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }
}
