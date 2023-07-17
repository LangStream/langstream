package com.datastax.oss.sga.deployer.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.k8s.api.PodAgentConfiguration;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AgentResourcesFactoryTest {

    @Test
    void testStatefulset() {
        final PodAgentConfiguration podConf = new PodAgentConfiguration(
                Map.of("input", Map.of("is_input", true)),
                Map.of("output", Map.of("is_output", true)),
                new PodAgentConfiguration.AgentConfiguration("agent-id", "my-agent", "FUNCTION", Map.of("config", true)),
                new StreamingCluster("noop", Map.of("config", true))
        );
        final AgentCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Agent
                metadata:
                  name: test-agent1
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    configuration: '%s'
                    tenant: my-tenant
                    applicationId: the-app
                """.formatted(SerializationUtil.writeAsJson(podConf)));
        final StatefulSet statefulSet = AgentResourcesFactory.generateStatefulSet(resource);
        assertEquals("""
                ---
                apiVersion: apps/v1
                kind: StatefulSet
                metadata:
                  labels:
                    app: sga-runtime
                    tenant: my-tenant
                  name: test-agent1
                  namespace: default
                spec:
                  podManagementPolicy: Parallel
                  replicas: 1
                  selector:
                    matchLabels:
                      app: sga-runtime
                      tenant: my-tenant
                  template:
                    metadata:
                      labels:
                        app: sga-runtime
                        tenant: my-tenant
                    spec:
                      containers:
                      - args:
                        - agent-runtime
                        - /app-config/config
                        image: ubuntu
                        imagePullPolicy: Always
                        name: runtime
                        volumeMounts:
                        - mountPath: /app-config
                          name: app-config
                      initContainers:
                      - args:
                        - "echo '{\\"input\\":{\\"input\\":{\\"is_input\\":true}},\\"output\\":{\\"output\\":{\\"is_output\\":true}},\\"agent\\":{\\"componentType\\":\\"FUNCTION\\",\\"agentId\\":\\"agent-id\\",\\"applicationId\\":\\"the-app\\",\\"agentType\\":\\"my-agent\\",\\"configuration\\":{\\"config\\":true}},\\"streamingCluster\\":{\\"type\\":\\"noop\\",\\"configuration\\":{\\"config\\":true}}}' > /app-config/config"
                        command:
                        - bash
                        - -c
                        image: ubuntu
                        imagePullPolicy: Always
                        name: runtime-init-config
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

    private AgentCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, AgentCustomResource.class);
    }
}