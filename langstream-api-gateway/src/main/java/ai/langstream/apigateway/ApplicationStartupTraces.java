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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

final class ApplicationStartupTraces {

    private static final String SEPARATOR = "-".repeat(58);
    private static final String BREAK = "\n";

    private static final Logger log = LoggerFactory.getLogger(ApplicationStartupTraces.class);

    private ApplicationStartupTraces() {}

    static String of(Environment environment) {
        Objects.requireNonNull(environment, "Environment must not be null");

        return new ApplicationStartupTracesBuilder()
                .append(BREAK)
                .appendSeparator()
                .append(applicationRunningTrace(environment))
                .append(localUrl(environment))
                .append(externalUrl(environment))
                .append(profilesTrace(environment))
                .appendSeparator()
                .append(configServer(environment))
                .build();
    }

    private static String applicationRunningTrace(Environment environment) {
        String applicationId = environment.getProperty("spring.application.name");

        if (StringUtils.isBlank(applicationId)) {
            return "Application is running!";
        }

        return "Application '%s' is running!".formatted(applicationId);
    }

    private static String localUrl(Environment environment) {
        return url("Local", "localhost", environment);
    }

    private static String externalUrl(Environment environment) {
        return url("External", hostAddress(), environment);
    }

    private static String url(String type, String host, Environment environment) {
        if (notWebEnvironment(environment)) {
            return null;
        }

        return "%s: \t%s://%s:%s%s"
                .formatted(
                        type,
                        protocol(environment),
                        host,
                        port(environment),
                        contextPath(environment));
    }

    private static boolean notWebEnvironment(Environment environment) {
        return StringUtils.isBlank(environment.getProperty("server.port"));
    }

    private static String protocol(Environment environment) {
        if (noKeyStore(environment)) {
            return "http";
        }

        return "https";
    }

    private static boolean noKeyStore(Environment environment) {
        return StringUtils.isBlank(environment.getProperty("server.ssl.key-store"));
    }

    private static String port(Environment environment) {
        return environment.getProperty("server.port");
    }

    private static String profilesTrace(Environment environment) {
        String[] profiles = environment.getActiveProfiles();

        if (ArrayUtils.isEmpty(profiles)) {
            return null;
        }

        return "Profile(s): \t%s".formatted(String.join(", ", profiles));
    }

    private static String hostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.warn("The host name could not be determined, using `localhost` as fallback");
        }

        return "localhost";
    }

    private static String contextPath(Environment environment) {
        String contextPath = environment.getProperty("server.servlet.context-path");

        if (StringUtils.isBlank(contextPath)) {
            return "/";
        }

        return contextPath;
    }

    private static String configServer(Environment environment) {
        String configServer = environment.getProperty("configserver.status");

        if (StringUtils.isBlank(configServer)) {
            return null;
        }

        return "Config Server: %s%s%s%s".formatted(configServer, BREAK, SEPARATOR, BREAK);
    }

    private static class ApplicationStartupTracesBuilder {

        private static final String SPACER = "  ";

        private final StringBuilder trace = new StringBuilder();

        public ApplicationStartupTracesBuilder appendSeparator() {
            trace.append(SEPARATOR).append(BREAK);

            return this;
        }

        public ApplicationStartupTracesBuilder append(String line) {
            if (line == null) {
                return this;
            }

            trace.append(SPACER).append(line).append(BREAK);

            return this;
        }

        public String build() {
            return trace.toString();
        }
    }
}
