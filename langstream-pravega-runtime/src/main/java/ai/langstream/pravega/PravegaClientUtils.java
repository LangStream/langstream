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
package ai.langstream.pravega;

import static ai.langstream.pravega.PravegaStreamingClusterRuntime.getPravegaClusterRuntimeConfiguration;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import java.net.URI;
import java.util.Map;

public class PravegaClientUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static StreamManager buildStreamManager(
            PravegaClusterRuntimeConfiguration pravegaClusterRuntimeConfiguration)
            throws Exception {
        Map<String, Object> clientConfig = pravegaClusterRuntimeConfiguration.client();
        String controllerUri =
                ConfigurationUtils.getString(
                        "controller-uri", "tcp://localhost:9090", clientConfig);

        return StreamManager.create(new URI(controllerUri));
    }

    public static StreamManager buildStreamManager(StreamingCluster streamingCluster)
            throws Exception {
        return buildStreamManager(getPravegaClusterRuntimeConfiguration(streamingCluster));
    }

    public static ReaderGroupManager buildReaderGroupManager(
            PravegaClusterRuntimeConfiguration pravegaClusterRuntimeConfiguration)
            throws Exception {
        Map<String, Object> clientConfig = pravegaClusterRuntimeConfiguration.client();
        String controllerUri =
                ConfigurationUtils.getString(
                        "controller-uri", "tcp://localhost:9090", clientConfig);

        return ReaderGroupManager.withScope("langstream", new URI(controllerUri));
    }

    public static ReaderGroupManager buildReaderGroupManager(StreamingCluster streamingCluster)
            throws Exception {
        return buildReaderGroupManager(getPravegaClusterRuntimeConfiguration(streamingCluster));
    }

    public static EventStreamClientFactory buildPravegaClient(StreamingCluster streamingCluster)
            throws Exception {

        final PravegaClusterRuntimeConfiguration pravegaClusterRuntimeConfiguration =
                getPravegaClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> clientConfig = pravegaClusterRuntimeConfiguration.client();
        String controllerUri =
                ConfigurationUtils.getString(
                        "controller-uri", "tcp://localhost:9090", clientConfig);
        String scope = ConfigurationUtils.getString("scope", "langstream", clientConfig);
        ClientConfig clientConfig1 =
                ClientConfig.builder().controllerURI(new URI(controllerUri)).build();

        return EventStreamClientFactory.withScope(scope, clientConfig1);
    }

    public static String getScope(PravegaClusterRuntimeConfiguration configuration) {
        return ConfigurationUtils.getString("scope", "langstream", configuration.client());
    }

    public static PravegaClusterRuntimeConfiguration getPravegarClusterRuntimeConfiguration(
            StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return MAPPER.convertValue(configuration, PravegaClusterRuntimeConfiguration.class);
    }
}
