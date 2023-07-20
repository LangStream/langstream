package com.datastax.oss.sga.webservice;

import com.datastax.oss.sga.impl.storage.LocalStore;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.nio.file.Files;
import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class WebAppTestConfig {

    @Bean
    @Primary
    @SneakyThrows
    public StorageProperties storageProperties() {
        return new StorageProperties(
                new StorageProperties.AppsStoreProperties("kubernetes", Map.of(
                        "namespaceprefix",
                        "sga-"
                )),
                new StorageProperties.GlobalMetadataStoreProperties("local",
                        Map.of(
                                LocalStore.LOCAL_BASEDIR,
                                Files.createTempDirectory("sga-test").toFile().getAbsolutePath()
                        )
                ),
                new StorageProperties.CodeStorageProperties()
        );
    }
}
