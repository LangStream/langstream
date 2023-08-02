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
package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.api.storage.ApplicationStoreRegistry;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import java.util.Objects;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationStoreFactory {

    @Bean
    public ApplicationStore getApplicationStore(StorageProperties storageProperties) {
        final StorageProperties.AppsStoreProperties apps = storageProperties.getApps();
        Objects.requireNonNull(apps);
        return ApplicationStoreRegistry.loadStore(apps.getType(), apps.getConfiguration());
    }
}
