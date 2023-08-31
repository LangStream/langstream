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

import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.webservice.common.GlobalMetadataService;
import ai.langstream.webservice.config.TenantProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class LangStreamEventListener implements ApplicationListener<ContextRefreshedEvent> {

    private final GlobalMetadataService globalMetadataService;
    private final TenantProperties tenantProperties;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (tenantProperties.getDefaultTenant() != null
                && tenantProperties.getDefaultTenant().isCreate()) {
            final String name = tenantProperties.getDefaultTenant().getName();
            log.info("Creating default tenant {}", name);
            globalMetadataService.putTenant(name, TenantConfiguration.builder().name(name).build());
        } else {
            log.info("No default tenant configured");
        }
        globalMetadataService.syncTenantsConfiguration();
    }
}
