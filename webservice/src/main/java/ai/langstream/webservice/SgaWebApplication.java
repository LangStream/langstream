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
package ai.langstream.webservice;

import ai.langstream.webservice.config.AuthTokenProperties;
import ai.langstream.webservice.config.StorageProperties;
import ai.langstream.webservice.config.TenantProperties;
import ai.langstream.webservice.config.ApplicationDeployProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.env.Environment;

@SpringBootApplication
@EnableConfigurationProperties({
		SgaProperties.class,
		StorageProperties.class,
		TenantProperties.class,
		AuthTokenProperties.class,
		ApplicationDeployProperties.class
})
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
