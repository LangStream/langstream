package com.datastax.oss.sga.apigateway;

import com.datastax.oss.sga.apigateway.config.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.env.Environment;

@SpringBootApplication
@EnableConfigurationProperties({ StorageProperties.class })
public class SgaApiGateway {

	static {
		java.security.Security.setProperty("networkaddress.cache.ttl", "1");
	}

	private static final Logger log = LoggerFactory.getLogger(SgaApiGateway.class);

	public static void main(String[] args) {
		Environment env = SpringApplication.run(SgaApiGateway.class, args).getEnvironment();
	}

}
