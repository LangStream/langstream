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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Secrets;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpecOptions;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.shaded.org.awaitility.Awaitility;

class KubernetesApplicationStoreTest {

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testApp() {

        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(getInitMap());

        final String tenant = getTenant();
        store.onTenantCreated(tenant);
        assertNotNull(k3s.getClient().namespaces().withName("s" + tenant).get());

        final Application app = new Application();
        app.setSecrets(
                new Secrets(
                        Map.of(
                                "mysecret",
                                new ai.langstream.api.model.Secret(
                                        "mysecret", "My secret", Map.of("token", "xxx")))));
        store.put(tenant, "myapp", app, "code-1", null, false, false);
        ApplicationCustomResource createdCr =
                k3s.getClient()
                        .resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get();

        assertNull(createdCr.getSpec().getImage());
        assertNull(createdCr.getSpec().getImagePullPolicy());
        assertEquals(
                "{\"resources\":{},\"modules\":{},\"instance\":null,\"gateways\":null,\"agentRunners\":{}}",
                createdCr.getSpec().getApplication());
        assertEquals(tenant, createdCr.getSpec().getTenant());
        assertEquals(
                "{\"deleteMode\":\"CLEANUP_REQUIRED\",\"markedForDeletion\":false,\"seed\":0,\"autoUpgradeRuntimeImage\":false,\"autoUpgradeRuntimeImagePullPolicy\":false,\"autoUpgradeAgentResources\":false,\"autoUpgradeAgentPodTemplate\":false}",
                createdCr.getSpec().getOptions());

        final Secret createdSecret =
                k3s.getClient().secrets().inNamespace("s" + tenant).withName("myapp").get();

        final OwnerReference ownerReference =
                createdSecret.getMetadata().getOwnerReferences().get(0);
        assertEquals("myapp", ownerReference.getName());
        assertEquals(createdCr.getKind(), ownerReference.getKind());
        assertEquals(createdCr.getApiVersion(), ownerReference.getApiVersion());
        assertEquals(createdCr.getMetadata().getUid(), ownerReference.getUid());
        assertTrue(ownerReference.getBlockOwnerDeletion());
        assertTrue(ownerReference.getController());
        assertEquals(
                "eyJzZWNyZXRzIjp7Im15c2VjcmV0Ijp7ImlkIjoibXlzZWNyZXQiLCJuYW1lIjoiTXkgc2VjcmV0IiwiZGF0YSI6eyJ0b2tlbiI6Inh4eCJ9fX19",
                createdSecret.getData().get("secrets"));

        assertEquals(1, store.list(tenant).size());

        store.put(tenant, "myapp", app, "code-1", null, true, false);
        createdCr =
                k3s.getClient()
                        .resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get();
        assertEquals(
                "{\"deleteMode\":\"CLEANUP_REQUIRED\",\"markedForDeletion\":false,\"seed\":0,\"autoUpgradeRuntimeImage\":true,\"autoUpgradeRuntimeImagePullPolicy\":true,\"autoUpgradeAgentResources\":true,\"autoUpgradeAgentPodTemplate\":true}",
                createdCr.getSpec().getOptions());

        store.put(tenant, "myapp", app, "code-1", null, true, true);
        createdCr =
                k3s.getClient()
                        .resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get();
        assertEquals(
                "{\"deleteMode\":\"CLEANUP_REQUIRED\",\"markedForDeletion\":false,\"seed\":%s,\"autoUpgradeRuntimeImage\":true,\"autoUpgradeRuntimeImagePullPolicy\":true,\"autoUpgradeAgentResources\":true,\"autoUpgradeAgentPodTemplate\":true}"
                        .formatted(
                                ApplicationSpec.deserializeOptions(createdCr.getSpec().getOptions())
                                                .getSeed()
                                        + ""),
                createdCr.getSpec().getOptions());

        assertNotEquals(
                "0",
                ApplicationSpec.deserializeOptions(createdCr.getSpec().getOptions()).getSeed()
                        + "");

        assertNotNull(store.get(tenant, "myapp", false));
        store.delete(tenant, "myapp", false);

        assertNotNull(
                k3s.getClient()
                        .resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get());

        // actually in the real deployment, the operator will intercept the app custom resource
        // deletion until the cleanup job is finished
        assertNotNull(k3s.getClient().secrets().inNamespace("s" + tenant).withName("myapp").get());

        store.onTenantDeleted(tenant);
        assertTrue(k3s.getClient().namespaces().withName("s" + tenant).get().isMarkedForDeletion());
    }

    @NotNull
    private Map<String, Object> getInitMap() {
        return Map.of("namespaceprefix", "s", "controlPlaneUrl", "http://localhost:8090");
    }

    static final AtomicInteger t = new AtomicInteger();

    String getTenant() {
        return "t" + t.incrementAndGet();
    }

    @Test
    void testBlockDeployWhileDeleting() {

        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(getInitMap());

        final String tenant = getTenant();
        store.onTenantCreated(tenant);
        final Application app = new Application();
        store.put(tenant, "myapp", app, "code-1", null, false, false);
        ApplicationCustomResource applicationCustomResource =
                k3s.getClient()
                        .resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get();
        applicationCustomResource.getMetadata().setFinalizers(List.of("test-finalizer"));
        k3s.getClient().resource(applicationCustomResource).update();
        store.delete(tenant, "myapp", false);
        applicationCustomResource = k3s.getClient().resource(applicationCustomResource).get();
        assertFalse(applicationCustomResource.isMarkedForDeletion());
        assertTrue(
                ApplicationSpec.deserializeOptions(applicationCustomResource.getSpec().getOptions())
                        .isMarkedForDeletion());
        assertEquals(
                ApplicationSpecOptions.DeleteMode.CLEANUP_REQUIRED,
                ApplicationSpec.deserializeOptions(applicationCustomResource.getSpec().getOptions())
                        .getDeleteMode());

        store.delete(tenant, "myapp", true);
        applicationCustomResource = k3s.getClient().resource(applicationCustomResource).get();
        assertFalse(applicationCustomResource.isMarkedForDeletion());
        assertTrue(
                ApplicationSpec.deserializeOptions(applicationCustomResource.getSpec().getOptions())
                        .isMarkedForDeletion());
        assertEquals(
                ApplicationSpecOptions.DeleteMode.CLEANUP_BEST_EFFORT,
                ApplicationSpec.deserializeOptions(applicationCustomResource.getSpec().getOptions())
                        .getDeleteMode());

        try {
            store.put(tenant, "myapp", app, "code-1", null, false, false);
            fail();
        } catch (IllegalArgumentException aie) {
            assertEquals("Application myapp is marked for deletion.", aie.getMessage());
        }
        applicationCustomResource.getMetadata().setFinalizers(List.of());
        k3s.getClient().resource(applicationCustomResource).update();
        k3s.getClient().resource(applicationCustomResource).delete();

        Awaitility.await()
                .until(
                        () -> {
                            try {
                                store.put(tenant, "myapp", app, "code-1", null, false, false);
                                return true;
                            } catch (IllegalArgumentException e) {
                                return false;
                            }
                        });
    }
}
