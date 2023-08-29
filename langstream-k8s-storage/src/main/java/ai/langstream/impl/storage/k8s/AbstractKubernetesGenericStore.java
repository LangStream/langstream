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
package ai.langstream.impl.storage.k8s;

import ai.langstream.api.storage.GenericStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKubernetesGenericStore<T extends HasMetadata>
        implements GenericStore {

    protected static final ObjectMapper mapper = new ObjectMapper();
    protected static final String PREFIX = "langstream-";

    private static String resourceName(String key) {
        return PREFIX + key;
    }

    @Override
    public String storeType() {
        return "kubernetes";
    }

    protected abstract KubernetesClient getClient();

    protected abstract T createResource(String key, String value);

    protected abstract MixedOperation<T, ? extends KubernetesResourceList<T>, Resource<T>>
            operation();

    protected abstract String get(String key, T resource);

    @SneakyThrows
    protected void putInNamespace(String namespace, String key, String value) {
        final T resource = createResource(key, value);
        final ObjectMeta metadata =
                new ObjectMetaBuilder()
                        .withName(resourceName(key))
                        .withLabels(Map.of("app", "langstream", "langstream-key", key))
                        .build();
        resource.setMetadata(metadata);
        getClient().resource(resource).inNamespace(namespace).serverSideApply();
    }

    @SneakyThrows
    protected void deleteInNamespace(String namespace, String key) {
        operation().inNamespace(namespace).withName(resourceName(key)).delete();
    }

    @SneakyThrows
    protected String getInNamespace(String namespace, String key) {
        final T resource = operation().inNamespace(namespace).withName(resourceName(key)).get();
        if (resource == null) {
            return null;
        }
        return get(key, resource);
    }

    @SneakyThrows
    protected LinkedHashMap<String, String> listInNamespace(String namespace) {
        return operation()
                .inNamespace(namespace)
                .withLabel("app", "langstream")
                .list()
                .getItems()
                .stream()
                .collect(
                        Collectors.toMap(
                                cf -> cf.getMetadata().getLabels().get("langstream-key"),
                                cf -> get(cf.getMetadata().getLabels().get("langstream-key"), cf),
                                (a, b) -> b,
                                LinkedHashMap::new));
    }
}
