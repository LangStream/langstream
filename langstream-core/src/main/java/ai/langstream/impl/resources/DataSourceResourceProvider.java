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
package ai.langstream.impl.resources;

import static ai.langstream.api.util.ConfigurationUtils.requiredField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;
import static ai.langstream.api.util.ConfigurationUtils.validateEnumField;
import static ai.langstream.api.util.ConfigurationUtils.validateInteger;

import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class DataSourceResourceProvider implements ResourceNodeProvider {
    @Override
    public Map<String, Object> createImplementation(
            Resource resource,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Map<String, Object> configuration = resource.configuration();

        String service = requiredField(configuration, "service", describe(resource));
        validateEnumField(
                configuration, "service", Set.of("astra", "cassandra"), describe(resource));

        switch (service) {
            case "astra":
                validateAstraDatabaseResource(resource);
                break;
            case "cassandra":
                validateCassandraDatabaseResource(resource);
                break;
            case "jdbc":
                validateJDBCDatabaseResource(resource);
            default:
                throw new IllegalStateException();
        }
        return resource.configuration();
    }

    private void validateJDBCDatabaseResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();
        requiredNonEmptyField(configuration, "url", describe(resource));
        requiredNonEmptyField(configuration, "driverClass", describe(resource));
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return "datasource".equals(type);
    }

    protected void validateAstraDatabaseResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();

        String secureBundle = ConfigurationUtils.getString("secureBundle", "", configuration);
        if (secureBundle.isEmpty()) {
            requiredNonEmptyField(configuration, "token", describe(resource));
            requiredNonEmptyField(configuration, "database", describe(resource));
        } else {
            if (secureBundle.startsWith("base64:")) {
                secureBundle = secureBundle.substring("base64:".length());
            }
            try {
                Base64.getDecoder().decode(secureBundle);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid base64 encoding for secureBundle in " + describe(resource), e);
            }
        }

        String username =
                ConfigurationUtils.getString(
                        "clientId",
                        ConfigurationUtils.getString("username", "", configuration),
                        configuration);
        if (username.isEmpty()) {
            requiredNonEmptyField(configuration, "clientId", describe(resource));
        }

        String password =
                ConfigurationUtils.getString(
                        "secret",
                        ConfigurationUtils.getString("password", "", configuration),
                        configuration);
        if (password.isEmpty()) {
            requiredNonEmptyField(configuration, "secret", describe(resource));
        }
    }

    protected void validateCassandraDatabaseResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();

        String secureBundle = ConfigurationUtils.getString("secureBundle", "", configuration);
        if (!secureBundle.isEmpty()) {
            throw new IllegalArgumentException(
                    "secureBundle is not supported for Cassandra services, use service=astra instead");
        }

        // in Cassandra testes you can use a Cassandra service without authentication
        requiredField(configuration, "username", describe(resource));
        requiredField(configuration, "password", describe(resource));

        requiredNonEmptyField(configuration, "contact-points", describe(resource));
        requiredNonEmptyField(configuration, "loadBalancing-localDc", describe(resource));
        validateInteger(configuration, "port", 1, 65535, describe(resource));
    }

    protected static Supplier<String> describe(Resource resource) {
        return () -> "resource with id = " + resource.id() + " of type " + resource.type();
    }
}
