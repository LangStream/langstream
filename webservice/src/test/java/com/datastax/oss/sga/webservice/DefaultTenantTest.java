package com.datastax.oss.sga.webservice;

import static org.junit.jupiter.api.Assertions.*;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import com.datastax.oss.sga.webservice.config.TenantProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@Import(WebAppTestConfig.class)
@DirtiesContext
class DefaultTenantTest {

    @Autowired
    GlobalMetadataService service;

    @MockBean
    ApplicationStore applicationStore;

    @Test
    public void testDefaultTenant() {
        System.out.println(service.listTenants());
        assertEquals(1, service.listTenants().size());
        assertEquals("default", service.getTenant("default").getName());
    }

}