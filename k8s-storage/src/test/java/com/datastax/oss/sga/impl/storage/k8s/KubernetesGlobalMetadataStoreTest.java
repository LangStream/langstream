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
package com.datastax.oss.sga.impl.storage.k8s;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.impl.k8s.KubernetesClientFactory;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class KubernetesGlobalMetadataStoreTest {


    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer();

    @Test
    public void testGlobalMetadataStore() throws Exception {
        final KubernetesClient client = k3s.getClient();
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
}
