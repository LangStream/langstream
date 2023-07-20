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