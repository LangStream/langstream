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
package ai.langstream.impl.storage.k8s.apps;

import static org.junit.jupiter.api.Assertions.*;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Secrets;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.shaded.org.awaitility.Awaitility;

class KubernetesApplicationStoreTest {

    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testApp() {

        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(Map.of("namespaceprefix", "s", "deployer-runtime", Map.of("image", "busybox", "image-pull-policy", "IfNotPresent")));

        final String tenant = getTenant();
        store.onTenantCreated(tenant);
        assertNotNull(k3s.getClient().namespaces().withName("s" + tenant).get());

        final Application app = new Application();
        app.setSecrets(new Secrets(Map.of("mysecret", new ai.langstream.api.model.Secret("mysecret", "My secret", Map.of("token", "xxx")))));
        store.put(tenant, "myapp", app, "code-1", null);
        final ApplicationCustomResource createdCr =
                k3s.getClient().resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get();

        assertEquals("IfNotPresent", createdCr.getSpec().getImagePullPolicy());
        assertEquals("{\"resources\":{},\"modules\":{},\"instance\":null,\"gateways\":null,\"agentRunners\":{}}", createdCr.getSpec().getApplication());
        assertEquals("busybox", createdCr.getSpec().getImage());
        assertEquals(tenant, createdCr.getSpec().getTenant());


        final Secret createdSecret = k3s.getClient().secrets()
                .inNamespace("s" + tenant)
                .withName("myapp")
                .get();

        final OwnerReference ownerReference = createdSecret.getMetadata().getOwnerReferences().get(0);
        assertEquals("myapp", ownerReference.getName());
        assertEquals(createdCr.getKind(), ownerReference.getKind());
        assertEquals(createdCr.getApiVersion(), ownerReference.getApiVersion());
        assertEquals(createdCr.getMetadata().getUid(), ownerReference.getUid());
        assertTrue(ownerReference.getBlockOwnerDeletion());
        assertTrue(ownerReference.getController());
        assertEquals("eyJzZWNyZXRzIjp7Im15c2VjcmV0Ijp7ImlkIjoibXlzZWNyZXQiLCJuYW1lIjoiTXkgc2VjcmV0IiwiZGF0YSI6eyJ0b2tlbiI6Inh4eCJ9fX19", createdSecret.getData().get("secrets"));

        assertEquals(1, store.list(tenant).size());

        assertNotNull(store.get(tenant, "myapp", false));
        store.delete(tenant, "myapp");

        assertNull(k3s.getClient().resources(ApplicationCustomResource.class)
                .inNamespace("s" + tenant)
                .withName("myapp")
                .get());

        // actually in the real deployment, the operator will intercept the app custom resource deletion until the cleanup job is finished
        assertNotNull(k3s.getClient().secrets()
                .inNamespace("s" + tenant)
                .withName("myapp")
                .get());

        Awaitility.await().untilAsserted(() -> {
            assertNull(k3s.getClient().secrets()
                    .inNamespace("s" + tenant)
                    .withName("myapp")
                    .get());
        });
        store.onTenantDeleted(tenant);
        assertTrue(k3s.getClient().namespaces().withName("s" + tenant).get().isMarkedForDeletion());
    }

    static final AtomicInteger t = new AtomicInteger();
    String getTenant() {
        return "t" + t.incrementAndGet();
    }

    @Test
    void testBlockDeployWhileDeleting() {

        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(Map.of("namespaceprefix", "s", "deployer-runtime", Map.of("image", "busybox", "image-pull-policy", "IfNotPresent")));

        final String tenant = getTenant();
        store.onTenantCreated(tenant);
        final Application app = new Application();
        store.put(tenant, "myapp", app, "code-1", null);
        ApplicationCustomResource createdCr =
                k3s.getClient().resources(ApplicationCustomResource.class)
                        .inNamespace("s" + tenant)
                        .withName("myapp")
                        .get();
        createdCr.getMetadata().setFinalizers(List.of("test-finalizer"));
        k3s.getClient().resource(createdCr).update();
        store.delete(tenant, "myapp");
        assertTrue(k3s.getClient().resource(createdCr).get().isMarkedForDeletion());

        try {
            store.put(tenant, "myapp", app, "code-1", null);
            fail();
        } catch (IllegalArgumentException aie) {
            assertEquals("Application myapp is marked for deletion. Please retry once the application is deleted.", aie.getMessage());
        }
        createdCr = k3s.getClient().resource(createdCr).get();
        createdCr.getMetadata().setFinalizers(List.of());
        k3s.getClient().resource(createdCr).update();

        Awaitility.await().until(() -> {
            try {
                store.put(tenant, "myapp", app, "code-1", null);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        });
    }
}
