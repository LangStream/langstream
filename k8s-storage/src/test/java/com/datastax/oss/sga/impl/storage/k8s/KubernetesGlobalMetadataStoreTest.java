package com.datastax.oss.sga.impl.storage.k8s;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.impl.storage.k8s.global.KubernetesGlobalMetadataStore;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
class KubernetesGlobalMetadataStoreTest {


    static K3sContainer k3s;
    static KubernetesClient client;
    static Path kubeconfigFile;

    @BeforeAll
    public static void setup(@TempDir Path tempDir) throws Exception {
        k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.21.3-k3s1"))
                .withLogConsumer(outputFrame -> log.info("k3s> {}", outputFrame.getUtf8String().trim()));
        k3s.start();
        kubeconfigFile = tempDir.resolve("kubeconfig.yaml");
        Files.writeString(kubeconfigFile, k3s.getKubeConfigYaml());
        System.setProperty(Config.KUBERNETES_KUBECONFIG_FILE, kubeconfigFile.toFile().getAbsolutePath());
        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(k3s.getKubeConfigYaml()))
                .build();
    }

    @Test
    public void testGlobalMetadataStore() throws Exception {
        final KubernetesGlobalMetadataStore store = new KubernetesGlobalMetadataStore();
        final String namespace = "default";
        store.initialize(Map.of("namespace", namespace));
        store.put("mykey", "myvalue");
        final ConfigMap configMap = client.configMaps().inNamespace(namespace).withName("sga-mykey").get();
        assertEquals("sga", configMap.getMetadata().getLabels().get("app"));
        assertEquals("mykey", configMap.getMetadata().getLabels().get("sga-key"));
        assertEquals("myvalue", configMap.getData().get("value"));
        assertEquals("myvalue", store.get("mykey"));
        final LinkedHashMap<String, String> list = store.list();
        assertEquals(1, list.size());
        assertEquals("myvalue", list.get("mykey"));
        store.delete("mykey");
        assertNull(client.configMaps().inNamespace(namespace).withName("sga-mykey").get());
    }


    @AfterAll
    public static void after() {
        System.clearProperty(Config.KUBERNETES_KUBECONFIG_FILE);
        if (k3s != null) {
            k3s.stop();
        }
    }
}