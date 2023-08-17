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
package ai.langstream.pulsar;

import ai.langstream.api.model.StreamingCluster;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.HashMap;
import java.util.Map;

public class PulsarClientUtils {
    public static PulsarAdmin buildPulsarAdmin(StreamingCluster streamingCluster) throws Exception {
        final PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration =
                getPulsarClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = pulsarClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        } else {
            adminConfig = new HashMap<>(adminConfig);
        }
        if (pulsarClusterRuntimeConfiguration.getAuthentication() != null) {
            adminConfig.putAll(pulsarClusterRuntimeConfiguration.getAuthentication());
        }
        if (adminConfig.get("serviceUrl") == null) {
            adminConfig.put("serviceUrl", "http://localhost:8080");
        }
        return PulsarAdmin
                .builder()
                .loadConf(adminConfig)
                .build();
    }

    public static PulsarClient buildPulsarClient(StreamingCluster streamingCluster) throws Exception {
        final PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration =
                getPulsarClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> clientConfig = pulsarClusterRuntimeConfiguration.getService();
        if (clientConfig == null) {
            clientConfig = new HashMap<>();
        } else {
            clientConfig = new HashMap<>(clientConfig);
        }
        if (pulsarClusterRuntimeConfiguration.getAuthentication() != null) {
            clientConfig.putAll(pulsarClusterRuntimeConfiguration.getAuthentication());
        }
        if (clientConfig.get("serviceUrl") == null) {
            clientConfig.put("serviceUrl", "pulsar://localhost:6650");
        }
        return PulsarClient
                .builder()
                .loadConf(clientConfig)
                .build();
    }

    public static PulsarClusterRuntimeConfiguration getPulsarClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return PulsarClusterRuntime.mapper.convertValue(configuration, PulsarClusterRuntimeConfiguration.class);
    }
}
