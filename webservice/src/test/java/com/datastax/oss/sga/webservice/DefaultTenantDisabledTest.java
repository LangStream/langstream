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
package com.datastax.oss.sga.webservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.impl.storage.LocalStore;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import com.datastax.oss.sga.webservice.config.TenantProperties;
import java.nio.file.Files;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(properties = {"application.tenants.default-tenant.create=false"})
@Import(WebAppTestConfig.class)
@DirtiesContext
class DefaultTenantDisabledTest {
    @Autowired
    GlobalMetadataService service;

    @MockBean
    ApplicationStore applicationStore;

    @Test
    public void testDefaultTenant() {
        System.out.println(service.listTenants());
        assertEquals(0, service.listTenants().size());
    }

}
