package com.datastax.oss.sga.impl.deploy;

import static org.mockito.ArgumentMatchers.any;
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import com.datastax.oss.sga.impl.storage.InMemoryConfigStore;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ApplicationManagerTest {

    @Test
    public void testDeploy() {
        final InMemoryConfigStore store = new InMemoryConfigStore();

        final ApplicationDeployer deployer = Mockito.mock(ApplicationDeployer.class);
        final ApplicationStore applicationStore = new ApplicationStore(store, store);
        final ApplicationManager manager =
                new ApplicationManager(deployer, applicationStore, 4);
        manager.deployApplication("test", new ApplicationInstance());

        Awaitility.await().untilAsserted(() -> {
            Mockito.verify(deployer, Mockito.times(1)).createImplementation(any());
            Mockito.verify(deployer, Mockito.times(1)).deploy(any(), any());
            final StoredApplicationInstance stored = applicationStore.get("test");
            Assertions.assertEquals(ApplicationInstanceLifecycleStatus.Status.DEPLOYED, stored.getStatus().getStatus());
        });

        manager.deleteApplication("test");
        Awaitility.await().untilAsserted(() -> {
            Assertions.assertNull(applicationStore.get("test"));
        });
    }

    @Test
    public void testDeployError() {
        final InMemoryConfigStore store = new InMemoryConfigStore();
        final ApplicationDeployer deployer = Mockito.mock(ApplicationDeployer.class);
        Mockito.doThrow(RuntimeException.class).when(deployer).createImplementation(any());
        final ApplicationStore applicationStore = new ApplicationStore(store, store);
        final ApplicationManager manager =
                new ApplicationManager(deployer, applicationStore, 4);
        manager.deployApplication("test", new ApplicationInstance());

        Awaitility.await().untilAsserted(() -> {
            Mockito.verify(deployer, Mockito.times(1)).createImplementation(any());
            Mockito.verify(deployer, Mockito.times(0)).deploy(any(), any());
            final StoredApplicationInstance stored = applicationStore.get("test");
            Assertions.assertEquals(ApplicationInstanceLifecycleStatus.Status.ERROR, stored.getStatus().getStatus());
        });

        manager.deleteApplication("test");
        Awaitility.await().untilAsserted(() -> {
            Assertions.assertNull(applicationStore.get("test"));
        });
    }

    @Test
    public void testRecovery() throws Exception {
        final InMemoryConfigStore store = new InMemoryConfigStore();


        final ApplicationDeployer deployer = Mockito.mock(ApplicationDeployer.class);
        final ApplicationStore applicationStore = new ApplicationStore(store, store);

        applicationStore.put("test", new ApplicationInstance(), ApplicationInstanceLifecycleStatus.CREATED);
        applicationStore.put("test-delete", new ApplicationInstance(), ApplicationInstanceLifecycleStatus.DELETING);

        final ApplicationManager manager =
                new ApplicationManager(deployer, applicationStore, 1);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Assertions.assertEquals(ApplicationInstanceLifecycleStatus.Status.DEPLOYED,
                    applicationStore.get("test").getStatus().getStatus());
            Assertions.assertNull(applicationStore.get("test-delete"));
        });
    }
}