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
package com.datastax.oss.sga.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.deployer.k8s.PodTemplate;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AgentResourcesFactoryTest {

    @Test
    void testStatefulset() {
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
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
        final StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(resource, Map.of(), new AgentResourceUnitConfiguration());
        assertEquals("""
                        ---
                        apiVersion: apps/v1
                        kind: StatefulSet
                        metadata:
                          labels:
                            app: sga-runtime
                            sga-agent: my-agent
                            sga-application: the-'app
                          name: test-agent1
                          namespace: default
                          ownerReferences:
                          - apiVersion: sga.oss.datastax.com/v1alpha1
                            kind: Agent
                            blockOwnerDeletion: true
                            controller: true
                            name: test-agent1
                        spec:
                          podManagementPolicy: Parallel
                          replicas: 1
                          selector:
                            matchLabels:
                              app: sga-runtime
                              sga-agent: my-agent
                              sga-application: the-'app
                          serviceName: test-agent1
                          template:
                            metadata:
                              annotations:
                                sga.com.datastax.oss/config-checksum: xx
                              labels:
                                app: sga-runtime
                                sga-agent: my-agent
                                sga-application: the-'app
                            spec:
                              containers:
                              - args:
                                - agent-runtime
                                - /app-config/config
                                - /app-code-download
                                image: busybox
                                imagePullPolicy: Never
                                name: runtime
                                ports:
                                - containerPort: 8080
                                  name: http
                                  protocol: TCP
                                resources:
                                  requests:
                                    cpu: 0.500000
                                    memory: 256M
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-code-download
                                  name: code-download
                              initContainers:
                              - args:
                                - "echo '{\\"tenant\\":\\"my-tenant\\",\\"type\\":\\"none\\",\\"codeStorageArchiveId\\":null,\\"configuration\\":{}}' > /code-config/config"
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
                                - mountPath: /code-config
                                  name: code-config
                              - args:
                                - agent-code-download
                                - /code-config/config
                                - /app-code-download
                                image: busybox
                                imagePullPolicy: Never
                                name: code-download
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /code-config
                                  name: code-config
                                - mountPath: /app-code-download
                                  name: code-download
                              terminationGracePeriodSeconds: 60
                              volumes:
                              - name: app-config
                                secret:
                                  items:
                                  - key: app-config
                                    path: config
                                  secretName: agent-config
                              - emptyDir: {}
                                name: code-config
                              - emptyDir: {}
                                name: code-download
                        """,
                SerializationUtil.writeAsYaml(statefulSet));
    }


    @Test
    void testResources() {
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
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
                AgentResourcesFactory.generateStatefulSet(resource, Map.of(), new AgentResourceUnitConfiguration());
        assertEquals(2, statefulSet.getSpec().getReplicas());
        final Container container = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(Quantity.parse("2"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("1024M"), container.getResources().getRequests().get("memory"));


    }

    @Test
    void testPodTemplate() {
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
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
        final PodTemplate podTemplate = new PodTemplate(List.of(new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue("sga")
                .withKey("workload")
                .build()), Map.of("workload", "sga"));
        final StatefulSet statefulSet =
                AgentResourcesFactory.generateStatefulSet(resource, Map.of(), new AgentResourceUnitConfiguration(),
                        podTemplate);
        final List<Toleration> tolerations = statefulSet.getSpec().getTemplate().getSpec().getTolerations();
        assertEquals(1, tolerations.size());
        final Toleration tol = tolerations.get(0);
        assertEquals("workload", tol.getKey());
        assertEquals("sga", tol.getValue());
        assertEquals("NoSchedule", tol.getEffect());
        assertEquals(Map.of("workload", "sga"), statefulSet.getSpec().getTemplate().getSpec().getNodeSelector());

    }
    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}
