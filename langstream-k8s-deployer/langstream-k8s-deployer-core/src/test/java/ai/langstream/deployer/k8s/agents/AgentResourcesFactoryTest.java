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
package ai.langstream.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.deployer.k8s.PodTemplate;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AgentResourcesFactoryTest {

    @Test
    void testStatefulsetAndService() {
        final AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    image: busybox-nope
                    imagePullPolicy: Always
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-'app
                    agentId: my-agent
                    options: '{"autoUpgradeRuntimeImage": true}'
                """);
        final StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .image("busybox")
                                .imagePullPolicy("Never")
                                .build());
        assertEquals(
                """
                        ---
                        apiVersion: apps/v1
                        kind: StatefulSet
                        metadata:
                          labels:
                            app: langstream-runtime
                            langstream-agent: my-agent
                            langstream-application: the-'app
                          name: test-agent1
                          namespace: default
                          ownerReferences:
                          - apiVersion: langstream.ai/v1alpha1
                            kind: Agent
                            blockOwnerDeletion: true
                            controller: true
                            name: test-agent1
                        spec:
                          podManagementPolicy: Parallel
                          replicas: 1
                          selector:
                            matchLabels:
                              app: langstream-runtime
                              langstream-agent: my-agent
                              langstream-application: the-'app
                          serviceName: test-agent1
                          template:
                            metadata:
                              annotations:
                                ai.langstream/application-seed: 0
                                ai.langstream/config-checksum: xx
                              labels:
                                app: langstream-runtime
                                langstream-agent: my-agent
                                langstream-application: the-'app
                            spec:
                              containers:
                              - args:
                                - agent-runtime
                                - /app-config/config
                                - /app-code-download
                                env:
                                - name: LANGSTREAM_AGENT_RUNNER_POD_CONFIGURATION
                                  value: /app-config/config
                                - name: LANGSTREAM_AGENT_RUNNER_CODE_PATH
                                  value: /app-code-download
                                image: busybox
                                imagePullPolicy: Never
                                livenessProbe:
                                  httpGet:
                                    path: /metrics
                                    port: http
                                  initialDelaySeconds: 10
                                  periodSeconds: 30
                                  timeoutSeconds: 5
                                name: runtime
                                ports:
                                - containerPort: 8080
                                  name: http
                                - containerPort: 8000
                                  name: service
                                readinessProbe:
                                  httpGet:
                                    path: /metrics
                                    port: http
                                  initialDelaySeconds: 10
                                  periodSeconds: 30
                                  timeoutSeconds: 5
                                resources:
                                  limits:
                                    cpu: 0.500000
                                    memory: 512M
                                  requests:
                                    cpu: 0.500000
                                    memory: 512M
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-code-download
                                  name: code-download
                              initContainers:
                              - args:
                                - "echo '{\\"codeDownloadPath\\":\\"/app-code-download\\",\\"tenant\\":\\"my-tenant\\",\\"applicationId\\":\\"the-'\\"'\\"'app\\",\\"codeArchiveId\\":null}' > /download-config/config"
                                command:
                                - bash
                                - -c
                                image: busybox
                                imagePullPolicy: Never
                                name: code-download-init
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 100Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /download-config
                                  name: download-config
                              - args:
                                - agent-code-download
                                env:
                                - name: LANGSTREAM_AGENT_CODE_DOWNLOADER_CLUSTER_CONFIGURATION
                                  value: /cluster-config/config
                                - name: LANGSTREAM_AGENT_CODE_DOWNLOADER_DOWNLOAD_CONFIGURATION
                                  value: /download-config/config
                                - name: LANGSTREAM_AGENT_CODE_DOWNLOADER_TOKEN
                                  value: /var/run/secrets/kubernetes.io/serviceaccount/token
                                image: busybox
                                imagePullPolicy: Never
                                name: code-download
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /cluster-config
                                  name: cluster-config
                                - mountPath: /download-config
                                  name: download-config
                                - mountPath: /app-code-download
                                  name: code-download
                              securityContext:
                                fsGroup: 10000
                              serviceAccountName: runtime-my-tenant
                              terminationGracePeriodSeconds: 60
                              volumes:
                              - name: app-config
                                secret:
                                  items:
                                  - key: app-config
                                    path: config
                                  secretName: agent-config
                              - name: cluster-config
                                secret:
                                  items:
                                  - key: cluster-config
                                    path: config
                                  secretName: langstream-cluster-config
                              - emptyDir: {}
                                name: code-download
                              - emptyDir: {}
                                name: download-config
                        """,
                SerializationUtil.writeAsYaml(statefulSet));

        Service service = AgentResourcesFactory.generateHeadlessService(resource);
        assertEquals(
                """
                ---
                apiVersion: v1
                kind: Service
                metadata:
                  labels:
                    app: langstream-runtime
                    langstream-agent: my-agent
                    langstream-application: the-'app
                  name: test-agent1
                  namespace: default
                  ownerReferences:
                  - apiVersion: langstream.ai/v1alpha1
                    kind: Agent
                    blockOwnerDeletion: true
                    controller: true
                    name: test-agent1
                spec:
                  clusterIP: None
                  ports:
                  - name: http
                    port: 8080
                  - name: service
                    port: 8000
                  selector:
                    app: langstream-runtime
                    langstream-agent: my-agent
                    langstream-application: the-'app
                """,
                SerializationUtil.writeAsYaml(service));
    }

    @Test
    void testResources() {
        final AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    image: busybox
                    imagePullPolicy: Never
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-'app
                    agentId: my-agent
                    resources:
                        parallelism: 2
                        size: 4
                """);
        final StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .build());
        assertEquals(2, statefulSet.getSpec().getReplicas());
        final Container container =
                statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(Quantity.parse("2"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("2048M"), container.getResources().getRequests().get("memory"));
    }

    @Test
    void testPodTemplate() {
        final AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    image: busybox
                    imagePullPolicy: Never
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-'app
                    agentId: my-agent
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
        final StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .podTemplate(podTemplate)
                                .build());
        final List<Toleration> tolerations =
                statefulSet.getSpec().getTemplate().getSpec().getTolerations();
        assertEquals(1, tolerations.size());
        final Toleration tol = tolerations.get(0);
        assertEquals("workload", tol.getKey());
        assertEquals("langstream", tol.getValue());
        assertEquals("NoSchedule", tol.getEffect());
        assertEquals(
                Map.of("workload", "langstream"),
                statefulSet.getSpec().getTemplate().getSpec().getNodeSelector());
        assertEquals(
                "value1",
                statefulSet.getSpec().getTemplate().getMetadata().getAnnotations().get("ann1"));
    }

    @Test
    void testDisks() {
        AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-app
                    agentId: my-agent
                    options: '{"disks":[{"agentId": "my-agent", "type": "default", "size": 888888}]}'
                """);
        final StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .image("busybox")
                                .imagePullPolicy("Never")
                                .build());

        PersistentVolumeClaim pvc = statefulSet.getSpec().getVolumeClaimTemplates().get(0);
        assertEquals("the-app-my-agent", pvc.getMetadata().getName());
        assertEquals("default", pvc.getMetadata().getNamespace());
        assertEquals("ReadWriteOnce", pvc.getSpec().getAccessModes().get(0));
        assertEquals("default", pvc.getSpec().getStorageClassName());
        assertEquals(
                Quantity.parse("888888"),
                pvc.getSpec().getResources().getRequests().get("storage"));

        final List<VolumeMount> mounts =
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getVolumeMounts();
        boolean found = false;
        for (VolumeMount mount : mounts) {
            if (mount.getName().equals("the-app-my-agent")) {
                found = true;
                assertEquals("/persistent-state/my-agent", mount.getMountPath());
            }
        }
        assertTrue(found);

        resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-app
                    agentId: my-agent
                    options: '{"disks":[{"agentId": "my-agent"}]}'
                """);
        pvc =
                AgentResourcesFactory.generateStatefulSet(
                                AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                        .agentCustomResource(resource)
                                        .image("busybox")
                                        .imagePullPolicy("Never")
                                        .build())
                        .getSpec()
                        .getVolumeClaimTemplates()
                        .get(0);

        assertEquals("default", pvc.getSpec().getStorageClassName());
        assertEquals(
                Quantity.parse("128M"), pvc.getSpec().getResources().getRequests().get("storage"));

        resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-app
                    agentId: my-agent
                    options: '{"disks":[{"agentId": "my-agent", "type": "custom-type"}]}'
                """);
        AgentResourceUnitConfiguration config = new AgentResourceUnitConfiguration();
        config.setStorageClassesMapping(Map.of("custom-type", "custom-storage-class"));
        config.setDefaultStorageDiskSize("1G");
        pvc =
                AgentResourcesFactory.generateStatefulSet(
                                AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                        .agentCustomResource(resource)
                                        .image("busybox")
                                        .imagePullPolicy("Never")
                                        .agentResourceUnitConfiguration(config)
                                        .build())
                        .getSpec()
                        .getVolumeClaimTemplates()
                        .get(0);

        assertEquals("custom-storage-class", pvc.getSpec().getStorageClassName());
        assertEquals(
                Quantity.parse("1G"), pvc.getSpec().getResources().getRequests().get("storage"));

        config = new AgentResourceUnitConfiguration();
        config.setStorageClassesMapping(Map.of("custom-type0", "custom-storage-class"));
        config.setDefaultStorageDiskSize("1G");
        pvc =
                AgentResourcesFactory.generateStatefulSet(
                                AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                        .agentCustomResource(resource)
                                        .image("busybox")
                                        .imagePullPolicy("Never")
                                        .agentResourceUnitConfiguration(config)
                                        .build())
                        .getSpec()
                        .getVolumeClaimTemplates()
                        .get(0);

        assertEquals("default", pvc.getSpec().getStorageClassName());
        assertEquals(
                Quantity.parse("1G"), pvc.getSpec().getResources().getRequests().get("storage"));
    }

    @Test
    void testProbes() {
        final AgentCustomResource resource =
                getCr(
                        """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    image: busybox
                    imagePullPolicy: Never
                    agentConfigSecretRef: agent-config
                    agentConfigSecretRefChecksum: xx
                    tenant: my-tenant
                    applicationId: the-'app
                    agentId: my-agent
                    resources:
                        parallelism: 2
                        size: 4
                """);

        final AgentResourceUnitConfiguration config = new AgentResourceUnitConfiguration();
        config.setEnableReadinessProbe(false);

        StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .agentResourceUnitConfiguration(config)
                                .build());
        assertNull(
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getReadinessProbe());
        assertNotNull(
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getLivenessProbe());
        config.setEnableReadinessProbe(true);
        config.setEnableLivenessProbe(false);

        statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .agentResourceUnitConfiguration(config)
                                .build());

        assertNull(
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getLivenessProbe());
        assertNotNull(
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getReadinessProbe());

        config.setEnableLivenessProbe(true);
        config.setLivenessProbeInitialDelaySeconds(25);
        config.setLivenessProbeTimeoutSeconds(10);
        config.setLivenessProbePeriodSeconds(60);

        config.setReadinessProbeInitialDelaySeconds(35);
        config.setReadinessProbeTimeoutSeconds(8);
        config.setReadinessProbePeriodSeconds(80);

        statefulSet =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(resource)
                                .agentResourceUnitConfiguration(config)
                                .build());

        final Probe liveness =
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getLivenessProbe();

        assertEquals(25, liveness.getInitialDelaySeconds());
        assertEquals(10, liveness.getTimeoutSeconds());
        assertEquals(60, liveness.getPeriodSeconds());

        final Probe readiness =
                statefulSet
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getReadinessProbe();

        assertEquals(35, readiness.getInitialDelaySeconds());
        assertEquals(8, readiness.getTimeoutSeconds());
        assertEquals(80, readiness.getPeriodSeconds());
    }

    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}
