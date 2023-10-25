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
package ai.langstream.impl.storage.k8s.apps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import ai.langstream.runtime.api.agent.AgentSpec;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class KubernetesApplicationStoreLogsTest {

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testLogs() throws Exception {
        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(
                Map.of(
                        "namespaceprefix",
                        "langstream-",
                        "controlPlaneUrl",
                        "http://localhost:8090"));
        store.onTenantCreated("mytenant");
        AgentCustomResource cr = agentCustomResource("mytenant", "myapp");
        k3s.getClient().resource(cr).inNamespace("langstream-mytenant").serverSideApply();
        cr = k3s.getClient().resource(cr).inNamespace("langstream-mytenant").get();
        deployMockStatefulset(cr);
        Awaitility.await()
                .untilAsserted(
                        () -> {
                            final List<ApplicationStore.PodLogHandler> podHandlers =
                                    store.logs(
                                            "mytenant", "myapp", new ApplicationStore.LogOptions());
                            assertEquals(2, podHandlers.size());
                        });
        List<ApplicationStore.PodLogHandler> podHandlers =
                store.logs("mytenant", "myapp", new ApplicationStore.LogOptions());

        final CountDownLatch done = new CountDownLatch(2);

        podHandlers
                .get(0)
                .start(
                        new ApplicationStore.LogLineConsumer() {
                            @Override
                            public ApplicationStore.LogLineResult onPodNotRunning(
                                    String state, String reason) {
                                log.info("Pod not running: {} {}", state, reason);
                                return new ApplicationStore.LogLineResult(true, 2L);
                            }

                            @Override
                            public ApplicationStore.LogLineResult onPodLogNotAvailable() {
                                log.info("Pod log n/a");
                                return new ApplicationStore.LogLineResult(true, null);
                            }

                            @Override
                            public ApplicationStore.LogLineResult onLogLine(
                                    String content, long timestamp) {
                                assertEquals("hello from myapp-agent111-0", content);
                                done.countDown();
                                return new ApplicationStore.LogLineResult(false, null);
                            }

                            @Override
                            public void onEnd() {}
                        });
        podHandlers
                .get(1)
                .start(
                        new ApplicationStore.LogLineConsumer() {

                            @Override
                            public ApplicationStore.LogLineResult onPodNotRunning(
                                    String state, String reason) {
                                log.info("Pod not running: {} {}", state, reason);
                                return new ApplicationStore.LogLineResult(true, 2L);
                            }

                            @Override
                            public ApplicationStore.LogLineResult onPodLogNotAvailable() {
                                log.info("Pod logs n/a");
                                return new ApplicationStore.LogLineResult(true, null);
                            }

                            @Override
                            public ApplicationStore.LogLineResult onLogLine(
                                    String content, long timestamp) {
                                assertEquals("hello from myapp-agent111-1", content);
                                done.countDown();
                                return new ApplicationStore.LogLineResult(false, null);
                            }

                            @Override
                            public void onEnd() {}
                        });

        done.await(30, TimeUnit.SECONDS);

        final CountDownLatch doneFiltered = new CountDownLatch(1);

        podHandlers =
                store.logs(
                        "mytenant",
                        "myapp",
                        new ApplicationStore.LogOptions(List.of("myapp-agent111-1")));
        assertEquals(1, podHandlers.size());

        podHandlers
                .get(0)
                .start(
                        new ApplicationStore.LogLineConsumer() {

                            @Override
                            public ApplicationStore.LogLineResult onPodNotRunning(
                                    String state, String reason) {
                                return new ApplicationStore.LogLineResult(true, null);
                            }

                            @Override
                            public ApplicationStore.LogLineResult onPodLogNotAvailable() {
                                return new ApplicationStore.LogLineResult(true, null);
                            }

                            @Override
                            public ApplicationStore.LogLineResult onLogLine(
                                    String content, long timestamp) {
                                assertEquals("hello from myapp-agent111-1", content);
                                doneFiltered.countDown();
                                return new ApplicationStore.LogLineResult(false, null);
                            }

                            @Override
                            public void onEnd() {}
                        });

        doneFiltered.await(30, TimeUnit.SECONDS);
    }

    private void deployMockStatefulset(AgentCustomResource cr) {
        final StatefulSet sts =
                AgentResourcesFactory.generateStatefulSet(
                        AgentResourcesFactory.GenerateStatefulsetParams.builder()
                                .agentCustomResource(cr)
                                .build());
        sts.getSpec().getTemplate().getSpec().getInitContainers().clear();
        sts.getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .setCommand(List.of("sh", "-c"));
        sts.getSpec().getTemplate().getSpec().getContainers().get(0).setReadinessProbe(null);
        sts.getSpec().getTemplate().getSpec().getContainers().get(0).setLivenessProbe(null);
        sts.getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .setArgs(
                        List.of(
                                "while true; do echo \"hello from $(hostname)\"; sleep 1000000; done"));
        k3s.getClient().resource(sts).inNamespace("langstream-mytenant").serverSideApply();
    }

    private static AgentCustomResource agentCustomResource(
            final String tenant, final String applicationId) {

        final String agentId = "agent111";
        final String agentCustomResourceName =
                AgentResourcesFactory.getAgentCustomResourceName(applicationId, agentId);

        k3s.getClient()
                .resource(
                        AgentResourcesFactory.generateAgentSecret(
                                agentCustomResourceName,
                                new RuntimePodConfiguration(
                                        Map.of("input", Map.of("is_input", true)),
                                        Map.of("output", Map.of("is_output", true)),
                                        new AgentSpec(
                                                AgentSpec.ComponentType.PROCESSOR,
                                                "my-tenant",
                                                agentId,
                                                "my-app",
                                                "fn-type",
                                                Map.of("config", true),
                                                Map.of(),
                                                Set.of()),
                                        new StreamingCluster("noop", Map.of("config", true)))))
                .inNamespace("langstream-" + tenant)
                .serverSideApply();

        return SerializationUtil.readYaml(
                """
                apiVersion: langstream.ai/v1alpha1
                kind: Agent
                metadata:
                  name: %s
                spec:
                    tenant: %s
                    applicationId: %s
                    agentId: %s
                    image: "busybox"
                    imagePullPolicy: IfNotPresent
                    agentConfigSecretRef: %s
                    agentConfigSecretRefChecksum: xx
                    resources:
                        size: 1
                        parallelism: 2
                """
                        .formatted(
                                agentCustomResourceName,
                                tenant,
                                applicationId,
                                agentId,
                                agentCustomResourceName),
                AgentCustomResource.class);
    }
}
