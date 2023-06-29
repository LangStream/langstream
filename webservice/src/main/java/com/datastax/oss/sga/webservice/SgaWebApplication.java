package com.datastax.oss.sga.webservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class SgaWebApplication {

	static {
		java.security.Security.setProperty("networkaddress.cache.ttl", "1");
	}

	private static final Logger log = LoggerFactory.getLogger(SgaWebApplication.class);

	public static void main(String[] args) {
		Environment env = SpringApplication.run(SgaWebApplication.class, args).getEnvironment();

		if (log.isInfoEnabled()) {
			log.info(ApplicationStartupTraces.of(env));
		}
	}

}
