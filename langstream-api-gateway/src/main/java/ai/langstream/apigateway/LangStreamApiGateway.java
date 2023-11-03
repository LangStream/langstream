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
package ai.langstream.apigateway;

import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.config.StorageProperties;
import ai.langstream.apigateway.config.TopicProperties;
import ai.langstream.apigateway.runner.CodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.env.Environment;

@SpringBootApplication
@EnableConfigurationProperties({
    StorageProperties.class,
    GatewayTestAuthenticationProperties.class,
    CodeConfiguration.class,
    TopicProperties.class
})
public class LangStreamApiGateway {

    static {
        java.security.Security.setProperty("networkaddress.cache.ttl", "1");
    }

    private static final Logger log = LoggerFactory.getLogger(LangStreamApiGateway.class);

    public static void main(String... args) {
        Environment env = SpringApplication.run(LangStreamApiGateway.class, args).getEnvironment();
        if (log.isInfoEnabled()) {
            log.info(ApplicationStartupTraces.of(env));
        }
    }
}
