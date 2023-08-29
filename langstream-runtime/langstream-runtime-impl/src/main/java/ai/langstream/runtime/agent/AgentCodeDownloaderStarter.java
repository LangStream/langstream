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
package ai.langstream.runtime.agent;

import ai.langstream.runtime.RuntimeStarter;
import ai.langstream.runtime.api.ClusterConfiguration;
import ai.langstream.runtime.api.agent.AgentCodeDownloaderConstants;
import ai.langstream.runtime.api.agent.DownloadAgentCodeConfiguration;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** This is the main entry point for the pods that run the LangStream runtime code downloader. */
@Slf4j
public class AgentCodeDownloaderStarter extends RuntimeStarter {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static MainErrorHandler mainErrorHandler =
            error -> {
                log.error("Unexpected error", error);
                System.exit(-1);
            };

    public interface MainErrorHandler {
        void handleError(Throwable error);
    }

    @SneakyThrows
    public static void main(String... args) {
        try {
            new AgentCodeDownloaderStarter(new AgentCodeDownloader()).start(args);
        } catch (Throwable error) {
            mainErrorHandler.handleError(error);
        }
    }

    private final AgentCodeDownloader agentCodeDownloader;

    public AgentCodeDownloaderStarter(AgentCodeDownloader agentCodeDownloader) {
        this.agentCodeDownloader = agentCodeDownloader;
    }

    @Override
    public void start(String... args) throws Exception {

        if (args.length > 0) {
            log.warn("args not supported, ignoring");
        }
        final Path downloadCodeConfigPath =
                getPathFromEnv(
                        AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV,
                        AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV_DEFAULT);
        final Path clusterConfigPath =
                getPathFromEnv(
                        AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV,
                        AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV_DEFAULT);
        final Path tokenPath =
                getOptionalPathFromEnv(
                        AgentCodeDownloaderConstants.TOKEN_ENV,
                        AgentCodeDownloaderConstants.TOKEN_ENV_DEFAULT);

        DownloadAgentCodeConfiguration configuration =
                MAPPER.readValue(
                        downloadCodeConfigPath.toFile(), DownloadAgentCodeConfiguration.class);

        ClusterConfiguration clusterConfiguration =
                MAPPER.readValue(clusterConfigPath.toFile(), ClusterConfiguration.class);
        final String token;
        if (tokenPath != null) {
            token = Files.readString(tokenPath);
        } else {
            token = null;
        }
        agentCodeDownloader.downloadCustomCode(clusterConfiguration, token, configuration);
    }
}
