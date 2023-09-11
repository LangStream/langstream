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

import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.Map;
import java.util.Set;

public class VectorDatabaseResourceProvider extends DataSourceResourceProvider
        implements ResourceNodeProvider {
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
                configuration,
                "service",
                Set.of("astra", "cassandra", "pinecone"),
                describe(resource));

        switch (service) {
            case "astra":
                validateAstraDatabaseResource(resource);
                break;
            case "cassandra":
                validateCassandraDatabaseResource(resource);
                break;
            case "pinecone":
                validatePineconeDatabaseResource(resource);
                break;
            default:
                throw new IllegalStateException();
        }

        return resource.configuration();
    }

    protected void validatePineconeDatabaseResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();

        requiredNonEmptyField(configuration, "api-key", describe(resource));
        requiredNonEmptyField(configuration, "environment", describe(resource));
        requiredNonEmptyField(configuration, "project-name", describe(resource));
        requiredNonEmptyField(configuration, "index-name", describe(resource));
        ConfigurationUtils.validateInteger(
                configuration, "server-side-timeout-sec", 1, 300000, describe(resource));
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return "vector-database".equals(type);
    }
}
