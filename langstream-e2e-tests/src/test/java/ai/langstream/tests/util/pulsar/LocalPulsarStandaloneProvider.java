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
package ai.langstream.tests.util.pulsar;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.StreamingClusterProvider;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;

@Slf4j
public class LocalPulsarStandaloneProvider implements StreamingClusterProvider {

    protected static final String NAMESPACE = "ls-test-pulsar";

    private final KubernetesClient client;
    private boolean started;

    public LocalPulsarStandaloneProvider(KubernetesClient client) {
        this.client = client;
    }

    @Override
    @SneakyThrows
    public StreamingCluster start() {
        if (!started) {
            internalStart();
            started = true;
        }

        return new StreamingCluster(
                "pulsar",
                Map.of(
                        "service",
                        Map.of(
                                "serviceUrl",
                                "pulsar://pulsar.%s.svc.cluster.local:6650".formatted(NAMESPACE)),
                        "admin",
                        Map.of(
                                "serviceUrl",
                                "http://pulsar.%s.svc.cluster.local:8080".formatted(NAMESPACE))));
    }

    private void internalStart() throws InterruptedException, IOException {
        log.info("installing pulsar on namespace {}", NAMESPACE);
        client.resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(NAMESPACE)
                                .endMetadata()
                                .build())
                .serverSideApply();

        Deployment deployment =
                new DeploymentBuilder()
                        .withNewMetadata()
                        .withName("pulsar")
                        .endMetadata()
                        .withNewSpec()
                        .withNewSelector()
                        .withMatchLabels(Map.of("app", "pulsar"))
                        .endSelector()
                        .withNewTemplate()
                        .withNewMetadata()
                        .withLabels(Map.of("app", "pulsar"))
                        .endMetadata()
                        .withNewSpec()
                        .withContainers(
                                new ContainerBuilder()
                                        .withName("pulsar")
                                        .withImage("apachepulsar/pulsar:3.2.2")
                                        .withCommand(
                                                List.of("bin/pulsar", "standalone", "-nss", "-nfw"))
                                        .withReadinessProbe(
                                                new ProbeBuilder()
                                                        .withNewExec()
                                                        .withCommand(
                                                                "sh",
                                                                "-c",
                                                                "curl -s --max-time %d --fail http://localhost:8080/admin/v2/brokers/health > /dev/null"
                                                                        .formatted(5))
                                                        .endExec()
                                                        .build())
                                        .withLivenessProbe(
                                                new ProbeBuilder()
                                                        .withNewExec()
                                                        .withCommand(
                                                                "sh",
                                                                "-c",
                                                                "curl -s --max-time %d --fail http://localhost:8080/admin/v2/brokers/health > /dev/null"
                                                                        .formatted(5))
                                                        .endExec()
                                                        .build())
                                        .build())
                        .endSpec()
                        .endTemplate()
                        .endSpec()
                        .build();
        Service service =
                new ServiceBuilder()
                        .withNewMetadata()
                        .withName("pulsar")
                        .endMetadata()
                        .withNewSpec()
                        .withPorts(
                                List.of(
                                        new ServicePortBuilder()
                                                .withName("http")
                                                .withPort(8080)
                                                .build(),
                                        new ServicePortBuilder()
                                                .withName("service")
                                                .withPort(6650)
                                                .build()))
                        .withSelector(Map.of("app", "pulsar"))
                        .endSpec()
                        .build();
        client.resource(service).inNamespace(NAMESPACE).serverSideApply();
        client.resource(deployment).inNamespace(NAMESPACE).serverSideApply();
        log.info("waiting pulsar to be ready");
        Awaitility.await()
                .pollInterval(java.time.Duration.ofSeconds(10))
                .pollDelay(Duration.ZERO)
                .atMost(2, TimeUnit.MINUTES)
                .until(
                        () -> {
                            final Deployment d =
                                    client.apps()
                                            .deployments()
                                            .inNamespace(NAMESPACE)
                                            .withName("pulsar")
                                            .get();
                            if (d == null) {
                                return false;
                            }
                            Pod pod = getPod();
                            if (pod == null) return false;
                            return BaseEndToEndTest.checkPodReadiness(pod);
                        });

        log.info("pulsar installed");
    }

    private Pod getPod() {
        final List<Pod> items =
                client.pods().inNamespace(NAMESPACE).withLabel("app", "pulsar").list().getItems();
        if (items.isEmpty()) {
            return null;
        }
        Pod pod = items.get(0);
        return pod;
    }

    @SneakyThrows
    private String execInPulsarPod(String cmd) {
        Pod pod = getPod();
        return BaseEndToEndTest.execInPodInNamespace(
                        NAMESPACE, pod.getMetadata().getName(), "pulsar", cmd.split(" "))
                .get(1, TimeUnit.MINUTES);
    }

    @Override
    public void cleanup() {
        List<String> topics = getTopics();
        for (String topic : topics) {
            log.info("deleting topic {}", topic);
            execInPulsarPod("bin/pulsar-admin topics delete -f %s".formatted(topic));
        }
    }

    @Override
    public List<String> getTopics() {
        final String result = execInPulsarPod("bin/pulsar-admin topics list public/default");
        if (result == null) {
            throw new IllegalStateException("failed to get topics from pulsar");
        }
        return result.lines()
                .map(topic -> topic.replace("persistent://public/default/", ""))
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        client.namespaces().withName(NAMESPACE).delete();
    }
}
