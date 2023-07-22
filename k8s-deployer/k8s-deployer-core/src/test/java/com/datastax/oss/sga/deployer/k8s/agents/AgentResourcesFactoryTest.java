package com.datastax.oss.sga.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
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
                                image: busybox
                                imagePullPolicy: Never
                                name: runtime
                                resources:
                                  requests:
                                    cpu: 0.500000
                                    memory: 256M
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                              terminationGracePeriodSeconds: 60
                              volumes:
                              - name: app-config
                                secret:
                                  items:
                                  - key: app-config
                                    path: config
                                  secretName: agent-config
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
    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}