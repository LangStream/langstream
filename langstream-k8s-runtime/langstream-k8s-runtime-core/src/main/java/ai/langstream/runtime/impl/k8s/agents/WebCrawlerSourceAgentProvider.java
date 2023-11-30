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
package ai.langstream.runtime.impl.k8s.agents;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.DiskSpec;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** Implements support for WebCrawler Source Agents. */
@Slf4j
public class WebCrawlerSourceAgentProvider extends AbstractComposableAgentProvider {

    public WebCrawlerSourceAgentProvider() {
        super(Set.of("webcrawler-source"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected DiskSpec computeDisk(
            AgentConfiguration agentConfiguration,
            Module module,
            Pipeline pipeline,
            ExecutionPlan physicalApplicationInstance,
            ComputeClusterRuntime clusterRuntime,
            StreamingClusterRuntime streamingClusterRuntime) {
        final String stateStorage =
                ConfigurationUtils.getString(
                        "state-storage", "s3", agentConfiguration.getConfiguration());
        if (stateStorage.equals("s3")) {
            return null;
        }
        if (!stateStorage.equals("disk")) {
            throw new IllegalArgumentException("Unsupported state-storage: " + stateStorage);
        }

        final DiskSpec configuredDisk =
                super.computeDisk(
                        agentConfiguration,
                        module,
                        pipeline,
                        physicalApplicationInstance,
                        clusterRuntime,
                        streamingClusterRuntime);
        String size = "256M";
        String type = null;
        if (configuredDisk != null) {
            if (configuredDisk.size() != null) {
                size = configuredDisk.size();
            }
            type = configuredDisk.type();
        }
        return new DiskSpec(true, type, size);
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return Config.class;
    }

    @Data
    @AgentConfig(
            name = "Web crawler source",
            description =
                    """
                    Crawl a website and extract the content of the pages.
                    """)
    public static class Config {
        @ConfigProperty(
                description =
                        """
                        State  storage configuration. "s3" or "disk"
                        """,
                defaultValue = "s3")
        @JsonProperty("state-storage")
        private String stateStorage;

        @ConfigProperty(
                description =
                        """
                                Configuration for handling the agent status.
                                The name of the bucket.
                                        """,
                defaultValue = "langstream-source")
        private String bucketName;

        @ConfigProperty(
                description =
                        """
                                Configuration for handling the agent status.
                                The S3 endpoint.""",
                defaultValue = "http://minio-endpoint.-not-set:9090")
        private String endpoint;

        @ConfigProperty(
                description =
                        """
                        Configuration for handling the agent status.
                        Access key for the S3 server.
                        """,
                defaultValue = "minioadmin")
        @JsonProperty("access-key")
        private String accessKey;

        @ConfigProperty(
                description =
                        """
                        Configuration for handling the agent status.
                        Secret key for the S3 server.
                        """,
                defaultValue = "minioadmin")
        @JsonProperty("secret-key")
        private String secretKey;

        @ConfigProperty(
                description =
                        """
                                Configuration for handling the agent status.
                                Region for the S3 server.
                                """)
        private String region = "";

        @ConfigProperty(
                description =
                        """
                                Domains that the crawler is allowed to access.
                                """)
        @JsonProperty("allowed-domains")
        private Set<String> allowedDomains;

        @ConfigProperty(
                description =
                        """
                                Paths that the crawler is not allowed to access.
                                """)
        @JsonProperty("forbidden-paths")
        private Set<String> forbiddenPaths;

        @ConfigProperty(
                description =
                        """
                                Maximum number of URLs that can be crawled.
                                """,
                defaultValue = "1000")
        @JsonProperty("max-urls")
        private int maxUrls;

        @ConfigProperty(
                description =
                        """
                                Maximum depth of the crawl.
                                """,
                defaultValue = "50")
        @JsonProperty("max-depth")
        private int maxDepth;

        @ConfigProperty(
                description =
                        """
                                Whether to scan the HTML documents to find links to other pages.
                                """,
                defaultValue = "true")
        @JsonProperty("handle-robots-file")
        private boolean handleRobotsFile;

        @ConfigProperty(
                description =
                        """
                Whether to scan HTML documents for links to other sites.
                                """,
                defaultValue = "true")
        @JsonProperty("scan-html-documents")
        private boolean scanHtmlDocuments;

        @ConfigProperty(
                description =
                        """
                Whether to emit non HTML documents to the pipeline (i.e. PDF Files).
                                """,
                defaultValue = "false")
        @JsonProperty("allow-non-html-contents")
        private boolean allowNonHtmlContents;

        @ConfigProperty(
                description =
                        """
                The starting URLs for the crawl.
                                """)
        @JsonProperty("seed-urls")
        private Set<String> seedUrls;

        @ConfigProperty(
                description =
                        """
                Time interval between reindexing of the pages.
                                """,
                defaultValue = (60 * 60 * 24) + "")
        @JsonProperty("reindex-interval-seconds")
        private int reindexIntervalSeconds;

        @ConfigProperty(
                description =
                        """
                        Maximum number of unflushed pages before the agent persists the crawl data.
                        """,
                defaultValue = "100")
        @JsonProperty("max-unflushed-pages")
        private int maxUnflushedPages;

        @ConfigProperty(
                description =
                        """
                        Minimum time between two requests to the same domain. (in milliseconds)
                        """,
                defaultValue = "500")
        @JsonProperty("min-time-between-requests")
        private int minTimeBetweenRequests;

        @ConfigProperty(
                description =
                        """
                        User agent to use for the requests.
                        """,
                defaultValue =
                        "Mozilla/5.0 (compatible; LangStream.ai/0.1; +https://langstream.ai)")
        @JsonProperty("user-agent")
        private String userAgent;

        @ConfigProperty(
                description =
                        """
                        Maximum number of errors allowed before stopping.
                        """,
                defaultValue = "5")
        @JsonProperty("max-error-count")
        private int maxErrorCount;

        @ConfigProperty(
                description =
                        """
                        Timeout for HTTP requests. (in milliseconds)
                        """,
                defaultValue = "10000")
        @JsonProperty("http-timeout")
        private int httpTimeout;

        @ConfigProperty(
                description =
                        """
                        Whether to handle cookies.
                        """,
                defaultValue = "true")
        @JsonProperty("handle-cookies")
        private boolean handleCookies;
    }
}
