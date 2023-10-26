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
package ai.langstream.pulsar;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record PulsarClusterRuntimeConfiguration(
        Map<String, Object> admin,
        Map<String, Object> service,
        Map<String, Object> authentication,
        @JsonProperty("default-tenant") String defaultTenant,
        @JsonProperty("default-namespace") String defaultNamespace,
        @JsonProperty("default-retention-policies") RetentionPolicies defaultRetentionPolicies) {
    public record RetentionPolicies(
            @JsonProperty("retention-time-in-minutes") Integer retentionTimeInMinutes,
            @JsonProperty("retention-size-in-mb") Long retentionSizeInMB) {}

    public PulsarClusterRuntimeConfiguration {
        if (defaultTenant == null) {
            defaultTenant = "public";
        }
        if (defaultNamespace == null) {
            defaultNamespace = "public";
        }
    }
}
