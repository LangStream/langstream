package com.datastax.oss.sga.webservice.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application.scheduling")
@Data
public class SchedulingProperties {

    private int executors = 4;

}
