package com.datastax.oss.sga.webservice;

import com.datastax.oss.sga.api.model.TenantConfiguration;
import com.datastax.oss.sga.webservice.common.GlobalMetadataService;
import com.datastax.oss.sga.webservice.config.TenantProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class SgaEventListener implements
        ApplicationListener<ContextRefreshedEvent> {

    private final GlobalMetadataService globalMetadataService;
    private final TenantProperties tenantProperties;


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        System.out.println("->" + tenantProperties);
        if (tenantProperties.getDefaultTenant() != null &&
                tenantProperties.getDefaultTenant().isCreate()) {
            final String name = tenantProperties.getDefaultTenant().getName();
            log.info("Creating default tenant {}", name);
            globalMetadataService.putTenant(name, TenantConfiguration.builder()
                    .name(name)
                    .build());
        } else {
            log.info("No default tenant configured");
        }


    }


}
