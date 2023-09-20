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
package ai.langstream.tests.util.kafka;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.StreamingClusterProvider;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalRedPandaClusterProvider implements StreamingClusterProvider {

    private static final boolean REUSE_EXISTING_REDPANDA =
            Boolean.parseBoolean(System.getProperty("langstream.tests.reuseRedPanda", "false"));
    protected static final String KAFKA_NAMESPACE = "kafka-ns";

    private final KubernetesClient client;

    public LocalRedPandaClusterProvider(KubernetesClient client) {
        this.client = client;
    }

    @Override
    @SneakyThrows
    public StreamingCluster start() {
        log.info("installing redpanda");
        client.resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(KAFKA_NAMESPACE)
                                .endMetadata()
                                .build())
                .serverSideApply();

        if (!REUSE_EXISTING_REDPANDA) {
            log.info("try to delete existing redpanda");
            BaseEndToEndTest.runProcess(
                    "helm delete redpanda --namespace kafka-ns".split(" "), true);
        }

        BaseEndToEndTest.runProcess(
                "helm repo add redpanda https://charts.redpanda.com/".split(" "), true);
        BaseEndToEndTest.runProcess("helm repo update".split(" "));
        // ref https://github.com/redpanda-data/helm-charts/blob/main/charts/redpanda/values.yaml
        log.info("running helm command to install redpanda");
        BaseEndToEndTest.runProcess(
                ("helm --debug upgrade --install redpanda redpanda/redpanda --namespace kafka-ns --set resources.cpu.cores=0.3"
                                + " --set resources.memory.container.max=1512Mi --set statefulset.replicas=1 --set console"
                                + ".enabled=false --set tls.enabled=false --set external.domain=redpanda-external.kafka-ns.svc"
                                + ".cluster.local --set statefulset.initContainers.setDataDirOwnership.enabled=true --set tuning.tune_aio_events=false --wait --timeout=5m")
                        .split(" "));
        log.info("waiting redpanda to be ready");
        BaseEndToEndTest.runProcess(
                "kubectl wait pods redpanda-0 --for=condition=Ready --timeout=5m -n kafka-ns"
                        .split(" "));
        log.info("redpanda installed");

        return new StreamingCluster(
                "kafka",
                Map.of(
                        "admin",
                        Map.of(
                                "bootstrap.servers",
                                "redpanda-0.redpanda.kafka-ns.svc.cluster.local:9093")));
    }

    @SneakyThrows
    private static String execInKafkaPod(String cmd) {
        return BaseEndToEndTest.execInPodInNamespace(
                        KAFKA_NAMESPACE, "redpanda-0", "redpanda", cmd.split(" "))
                .get(1, TimeUnit.MINUTES);
    }

    @Override
    public void cleanup() {
        execInKafkaPod("rpk topic delete -r \".*\"");
    }

    @Override
    public List<String> getTopics() {
        final String result = execInKafkaPod("rpk topic list");
        if (result == null) {
            throw new IllegalStateException("failed to get topics from kafka");
        }

        final List<String> topics = new ArrayList<>();
        final List<String> lines = result.lines().collect(Collectors.toList());
        boolean first = true;
        for (String line : lines) {
            if (first) {
                first = false;
                continue;
            }
            topics.add(line.split(" ")[0]);
        }
        return topics;
    }

    @Override
    public void stop() {}
}
