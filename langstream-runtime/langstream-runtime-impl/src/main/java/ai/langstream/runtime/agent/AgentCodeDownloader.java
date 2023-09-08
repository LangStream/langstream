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

import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.api.codestorage.LocalZipFileArchiveFile;
import ai.langstream.runtime.api.ClusterConfiguration;
import ai.langstream.runtime.api.agent.DownloadAgentCodeConfiguration;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/** This is the main entry point for downloading the application code. */
@Slf4j
public class AgentCodeDownloader {

    public void downloadCustomCode(
            ClusterConfiguration clusterConfiguration,
            String token,
            DownloadAgentCodeConfiguration configuration)
            throws Exception {
        log.info(
                "Downloading custom code {}, cluster configuration {}",
                configuration,
                clusterConfiguration);
        final String tenant = Objects.requireNonNull(configuration.tenant());
        final String codeDownloadPath = Objects.requireNonNull(configuration.codeDownloadPath());
        final String applicationId = Objects.requireNonNull(configuration.applicationId());
        final String codeArchiveId = Objects.requireNonNull(configuration.codeArchiveId());
        final Path destinationPath = Paths.get(codeDownloadPath);
        if (destinationPath.toFile().exists()) {
            destinationPath.toFile().mkdirs();
        }

        try (AdminClient adminClient = createAdminClient(clusterConfiguration, token, tenant)) {
            final HttpResponse<InputStream> download =
                    adminClient
                            .applications()
                            .download(
                                    applicationId,
                                    codeArchiveId,
                                    HttpResponse.BodyHandlers.ofInputStream());
            LocalZipFileArchiveFile.extractTo(download.body(), destinationPath);
            log.info("Downloaded code to {}", codeDownloadPath);
        }
    }

    private static AdminClient createAdminClient(
            ClusterConfiguration clusterConfiguration, String token, String tenant)
            throws Exception {
        final ai.langstream.admin.client.AdminClientConfiguration config =
                ai.langstream.admin.client.AdminClientConfiguration.builder()
                        .webServiceUrl(clusterConfiguration.controlPlaneUrl())
                        .token(token)
                        .tenant(tenant)
                        .build();

        final AdminClientLogger logger =
                new AdminClientLogger() {
                    @Override
                    public void log(Object message) {
                        log.info(message.toString());
                    }

                    @Override
                    public void error(Object message) {
                        log.error(message.toString());
                    }

                    @Override
                    public boolean isDebugEnabled() {
                        return log.isDebugEnabled();
                    }

                    @Override
                    public void debug(Object message) {
                        log.debug(message.toString());
                    }
                };
        return new AdminClient(config, logger);
    }
}
