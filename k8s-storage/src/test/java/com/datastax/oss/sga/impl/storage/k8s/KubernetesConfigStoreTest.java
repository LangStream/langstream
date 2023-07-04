package com.datastax.oss.sga.impl.storage.k8s;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.api.storage.ConfigStore;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
class KubernetesConfigStoreTest {


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

    @ParameterizedTest
    @ValueSource(classes = {ConfigMapKubernetesConfigStore.class, SecretsKubernetesConfigStore.class})
    public void test(Class storeClass) throws Exception {
        String namespace = "test-" + System.nanoTime();
        client.resource(new NamespaceBuilder()
                        .withNewMetadata()
                        .withName(namespace)
                        .endMetadata().build())
                .create();
        final ConfigStore store = (ConfigStore) storeClass.getConstructor().newInstance();
        store.initialize(Map.of("namespace", namespace));
        store.put("mykey", "myvalue");
        if (storeClass == ConfigMapKubernetesConfigStore.class) {
            final ConfigMap configMap = client.configMaps().inNamespace(namespace).withName("sga-mykey").get();
            assertEquals("sga", configMap.getMetadata().getLabels().get("app"));
            assertEquals("mykey", configMap.getMetadata().getLabels().get("sga-key"));
            assertEquals("myvalue", configMap.getData().get("value"));
            assertEquals("myvalue", store.get("mykey"));
            assertEquals(0, client.secrets().inNamespace(namespace).withLabel("app", "sga")
                    .list().getItems().size());
        } else {
            final Secret secret = client.secrets().inNamespace(namespace).withName("sga-mykey").get();
            assertEquals("sga", secret.getMetadata().getLabels().get("app"));
            assertEquals("mykey", secret.getMetadata().getLabels().get("sga-key"));
            assertEquals("bXl2YWx1ZQ==", secret.getData().get("value"));
            assertEquals("myvalue", store.get("mykey"));
            assertEquals(0, client.configMaps().inNamespace(namespace)
                    .withLabel("app", "sga")
                    .list().getItems().size());
        }
        final LinkedHashMap<String, String> list = store.list();
        assertEquals(1, list.size());
        assertEquals("myvalue", list.get("mykey"));
        store.delete("mykey");
        if (storeClass == ConfigMapKubernetesConfigStore.class) {
            assertNull(client.configMaps().inNamespace(namespace).withName("sga-mykey").get());
        } else {
            assertNull(client.secrets().inNamespace(namespace).withName("sga-mykey").get());
        }
    }


    @AfterAll
    public static void after() {
        System.clearProperty(Config.KUBERNETES_KUBECONFIG_FILE);
        if (k3s != null) {
            k3s.stop();
        }
    }
}