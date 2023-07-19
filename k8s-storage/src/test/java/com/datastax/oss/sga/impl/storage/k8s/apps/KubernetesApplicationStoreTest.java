package com.datastax.oss.sga.impl.storage.k8s.apps;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import java.util.Map;
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

        store.onTenantCreated("mytenant");
        assertNotNull(k3s.getClient().namespaces().withName("smytenant").get());

        final Application app = new Application();
        app.setSecrets(new Secrets(Map.of("mysecret", new com.datastax.oss.sga.api.model.Secret("mysecret", "My secret", Map.of("token", "xxx")))));
        store.put("mytenant", "myapp", app, "code-1");
        final ApplicationCustomResource createdCr =
                k3s.getClient().resources(ApplicationCustomResource.class)
                        .inNamespace("smytenant")
                        .withName("myapp")
                        .get();

        assertEquals("IfNotPresent", createdCr.getSpec().getImagePullPolicy());
        assertEquals("{\"resources\":{},\"modules\":{},\"instance\":null}", createdCr.getSpec().getApplication());
        assertEquals("busybox", createdCr.getSpec().getImage());
        assertEquals("mytenant", createdCr.getSpec().getTenant());


        final Secret createdSecret = k3s.getClient().secrets()
                .inNamespace("smytenant")
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

        assertEquals(1, store.list("mytenant").size());

        assertNotNull(store.get("mytenant", "myapp"));
        store.delete("mytenant", "myapp");

        assertNull(k3s.getClient().resources(ApplicationCustomResource.class)
                .inNamespace("smytenant")
                .withName("myapp")
                .get());

        // actually in the real deployment, the operator will intercept the app custom resource deletion until the cleanup job is finished
        assertNotNull(k3s.getClient().secrets()
                .inNamespace("smytenant")
                .withName("myapp")
                .get());

        Awaitility.await().untilAsserted(() -> {
            assertNull(k3s.getClient().secrets()
                    .inNamespace("smytenant")
                    .withName("myapp")
                    .get());
        });
        store.onTenantDeleted("mytenant");
        assertTrue(k3s.getClient().namespaces().withName("smytenant").get().isMarkedForDeletion());
    }
}