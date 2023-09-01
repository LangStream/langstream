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
package ai.langstream.webservice;

import ai.langstream.impl.storage.LocalStore;
import ai.langstream.webservice.config.StorageProperties;
import java.nio.file.Files;
import java.util.Map;
import lombok.SneakyThrows;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class WebAppTestConfig {

    @SneakyThrows
    public static StorageProperties buildStorageProperties() {
        return new StorageProperties(
                new StorageProperties.AppsStoreProperties(
                        "kubernetes", Map.of("namespaceprefix", "langstream-")),
                new StorageProperties.GlobalMetadataStoreProperties(
                        "local",
                        Map.of(
                                LocalStore.LOCAL_BASEDIR,
                                Files.createTempDirectory("langstream-test")
                                        .toFile()
                                        .getAbsolutePath())),
                new StorageProperties.CodeStorageProperties());
    }

    @Bean
    @Primary
    @SneakyThrows
    public StorageProperties storageProperties() {
        return buildStorageProperties();
    }
}
