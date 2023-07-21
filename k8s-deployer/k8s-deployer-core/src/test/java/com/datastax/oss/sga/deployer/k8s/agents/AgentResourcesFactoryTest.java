package com.datastax.oss.sga.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.k8s.api.PodAgentConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AgentResourcesFactoryTest {

    @Test
    void testStatefulset() {
        final PodAgentConfiguration podConf = new PodAgentConfiguration(
                "busybox",
                "IfNotPresent",
                new PodAgentConfiguration.ResourcesConfiguration(1, 1),
                Map.of("input", Map.of("is_input", true)),
                Map.of("output", Map.of("is_output", true)),
                new PodAgentConfiguration.AgentConfiguration("agent-id", "my-agent", "FUNCTION", Map.of("config", true)),
                new StreamingCluster("noop", Map.of("config", true)),
                new PodAgentConfiguration.CodeStorageConfiguration("code-storage-id")
        );
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    configuration: '%s'
                    tenant: my-tenant
                    applicationId: the-'app
                """.formatted(SerializationUtil.writeAsJson(podConf)));
        final StatefulSet statefulSet = AgentResourcesFactory.generateStatefulSet(resource, Map.of(), new AgentResourceUnitConfiguration());
        assertEquals("""
                        ---
                        apiVersion: apps/v1
                        kind: StatefulSet
                        metadata:
                          labels:
                            app: sga-runtime
                            sga-agent: agent-id
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
                              sga-agent: agent-id
                              sga-application: the-'app
                          template:
                            metadata:
                              labels:
                                app: sga-runtime
                                sga-agent: agent-id
                                sga-application: the-'app
                            spec:
                              containers:
                              - args:
                                - agent-runtime
                                - /app-config/config
                                image: busybox
                                imagePullPolicy: IfNotPresent
                                name: runtime
                                resources:
                                  requests:
                                    cpu: 0.500000
                                    memory: 256M
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                              initContainers:
                              - args:
                                - "echo '{\\"input\\":{\\"input\\":{\\"is_input\\":true}},\\"output\\":{\\"output\\":{\\"is_output\\":true}},\\"agent\\":{\\"componentType\\":\\"FUNCTION\\",\\"tenant\\":\\"my-tenant\\",\\"agentId\\":\\"agent-id\\",\\"applicationId\\":\\"the-'\\"'\\"'app\\",\\"agentType\\":\\"my-agent\\",\\"configuration\\":{\\"config\\":true}},\\"streamingCluster\\":{\\"type\\":\\"noop\\",\\"configuration\\":{\\"config\\":true}},\\"codeStorage\\":{\\"type\\":\\"none\\",\\"codeStorageArchiveId\\":\\"code-storage-id\\",\\"configuration\\":{}}}' > /app-config/config"
                                command:
                                - bash
                                - -c
                                image: busybox
                                imagePullPolicy: IfNotPresent
                                name: runtime-init-config
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 100Mi
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                              serviceAccountName: my-tenant
                              terminationGracePeriodSeconds: 60
                              volumes:
                              - emptyDir: {}
                                name: app-config
                        """,
                SerializationUtil.writeAsYaml(statefulSet));
    }


    @Test
    void testResources() {
        final PodAgentConfiguration podConf = new PodAgentConfiguration(
                "busybox",
                "IfNotPresent",
                new PodAgentConfiguration.ResourcesConfiguration(2, 4),
                Map.of("input", Map.of("is_input", true)),
                Map.of("output", Map.of("is_output", true)),
                new PodAgentConfiguration.AgentConfiguration("agent-id", "my-agent", "FUNCTION", Map.of("config", true)),
                new StreamingCluster("noop", Map.of("config", true)),
                new PodAgentConfiguration.CodeStorageConfiguration("")
        );
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    configuration: '%s'
                    tenant: my-tenant
                    applicationId: the-app
                """.formatted(SerializationUtil.writeAsJson(podConf)));
        final StatefulSet statefulSet = AgentResourcesFactory.generateStatefulSet(resource, Map.of(), new AgentResourceUnitConfiguration());
        assertEquals(2, statefulSet.getSpec().getReplicas());
        final Container container = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(Quantity.parse("2"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("1024M"), container.getResources().getRequests().get("memory"));


    }

    @Test
    void testResourcesNull() {
        final PodAgentConfiguration podConf = new PodAgentConfiguration(
                "busybox",
                "IfNotPresent",
                null,
                Map.of("input", Map.of("is_input", true)),
                Map.of("output", Map.of("is_output", true)),
                new PodAgentConfiguration.AgentConfiguration("agent-id", "my-agent", "FUNCTION", Map.of("config", true)),
                new StreamingCluster("noop", Map.of("config", true)),
                new PodAgentConfiguration.CodeStorageConfiguration("")
        );
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    configuration: '%s'
                    tenant: my-tenant
                    applicationId: the-app
                """.formatted(SerializationUtil.writeAsJson(podConf)));
        final StatefulSet statefulSet = AgentResourcesFactory.generateStatefulSet(resource, Map.of(), new AgentResourceUnitConfiguration());
        assertEquals(1, statefulSet.getSpec().getReplicas());
        final Container container = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertEquals(Quantity.parse("0.5"), container.getResources().getRequests().get("cpu"));
        assertEquals(Quantity.parse("256M"), container.getResources().getRequests().get("memory"));


    }

    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}