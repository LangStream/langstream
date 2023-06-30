package com.datastax.oss.sga.impl.storage;


import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
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
        final LocalStorageConfigStore configStore = new LocalStorageConfigStore();
        configStore.initialize(Map.of(LocalStorageConfigStore.LOCAL_BASEDIR, base.toFile().getAbsolutePath()));
        final ApplicationStore store = new ApplicationStore(configStore);


        Path path = Paths.get("../examples/application1");
        ApplicationInstance application = ModelBuilder.buildApplicationInstance(Arrays.asList(path));
        store.put("test", application, ApplicationInstanceLifecycleStatus.CREATED);
        StoredApplicationInstance get = store.get("test");
        Assertions.assertEquals("test", get.getName());
        Assertions.assertEquals(ApplicationInstanceLifecycleStatus.CREATED, get.getStatus());
        Assertions.assertEquals(mapper.writeValueAsString(application), mapper.writeValueAsString(get.getInstance()));
        get = store.list()
                .get("test");

        Assertions.assertEquals("test", get.getName());
        Assertions.assertEquals(ApplicationInstanceLifecycleStatus.CREATED, get.getStatus());
        Assertions.assertEquals(mapper.writeValueAsString(application), mapper.writeValueAsString(get.getInstance()));

        store.delete("test");
        Assertions.assertNull(store.get("test"));
        Assertions.assertEquals(0, store.list().size());
        // test no errors
        store.delete("test");
    }

}