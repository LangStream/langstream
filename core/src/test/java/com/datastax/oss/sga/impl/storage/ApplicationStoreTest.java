package com.datastax.oss.sga.impl.storage;


import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApplicationStoreTest {

    static ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    @Test
    public void test() throws Exception {
        final Path base = Files.createTempDirectory("sga-test");
        final LocalStore configStore = new LocalStore();
        configStore.initialize(Map.of(LocalStore.LOCAL_BASEDIR, base.toFile().getAbsolutePath()));
        final ApplicationStore store = new ApplicationStore(configStore, configStore);

        final String tenant = "tenant";
        store.initializeTenant(tenant);

        Path path = Paths.get("../examples/application1");
        ApplicationInstance application = ModelBuilder.buildApplicationInstance(Arrays.asList(path));
        store.put(tenant, "test", application, ApplicationInstanceLifecycleStatus.CREATED);
        Assertions.assertNotNull(configStore.get(tenant, "app-test"));
        Assertions.assertNotNull(configStore.get(tenant, "sec-test"));

        StoredApplicationInstance get = store.get(tenant, "test");
        Assertions.assertEquals("test", get.getName());
        Assertions.assertEquals(ApplicationInstanceLifecycleStatus.CREATED, get.getStatus());
        Assertions.assertEquals(mapper.writeValueAsString(application), mapper.writeValueAsString(get.getInstance()));
        get = store.list(tenant)
                .get("test");

        Assertions.assertEquals("test", get.getName());
        Assertions.assertEquals(ApplicationInstanceLifecycleStatus.CREATED, get.getStatus());
        Assertions.assertEquals(mapper.writeValueAsString(application), mapper.writeValueAsString(get.getInstance()));

        store.delete(tenant, "test");
        Assertions.assertNull(store.get(tenant, "test"));
        Assertions.assertEquals(0, store.list(tenant).size());
        // test no errors
        store.delete(tenant, "test");
    }

}