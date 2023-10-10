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
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** Implements support for S3/Azure Source Agents. */
@Slf4j
public class ObjectStorageSourceAgentProvider extends AbstractComposableAgentProvider {

    protected static final String AZURE_BLOB_STORAGE_SOURCE = "azure-blob-storage-source";
    protected static final String S3_SOURCE = "s3-source";

    public ObjectStorageSourceAgentProvider() {
        super(
                Set.of(S3_SOURCE, AZURE_BLOB_STORAGE_SOURCE),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        switch (type) {
            case S3_SOURCE:
                return S3SourceConfiguration.class;
            case AZURE_BLOB_STORAGE_SOURCE:
                return AzureBlobStorageConfiguration.class;
            default:
                throw new IllegalArgumentException("Unknown agent type: " + type);
        }
    }

    @AgentConfig(name = "S3 Source", description = "Reads data from S3 bucket")
    @Data
    public static class S3SourceConfiguration {

        protected static final String DEFAULT_BUCKET_NAME = "langstream-source";
        protected static final String DEFAULT_ENDPOINT = "http://minio-endpoint.-not-set:9090";
        protected static final String DEFAULT_ACCESSKEY = "minioadmin";
        protected static final String DEFAULT_SECRETKEY = "minioadmin";
        protected static final String DEFAULT_FILE_EXTENSIONS = "pdf,docx,html,htm,md,txt";

        @ConfigProperty(
                description =
                        """
                        The name of the bucket to read from.
                        """,
                defaultValue = DEFAULT_BUCKET_NAME)
        private String bucketName = DEFAULT_BUCKET_NAME;

        @ConfigProperty(
                description =
                        """
                        The endpoint of the S3 server.
                        """,
                defaultValue = DEFAULT_ENDPOINT)
        private String endpoint = DEFAULT_ENDPOINT;

        @ConfigProperty(
                description =
                        """
                        Access key for the S3 server.
                        """,
                defaultValue = DEFAULT_ACCESSKEY)
        @JsonProperty("access-key")
        private String accessKey = DEFAULT_ACCESSKEY;

        @ConfigProperty(
                description =
                        """
                        Secret key for the S3 server.
                        """,
                defaultValue = DEFAULT_SECRETKEY)
        @JsonProperty("secret-key")
        private String secretKey = DEFAULT_SECRETKEY;

        @ConfigProperty(
                required = false,
                description =
                        """
                                Region for the S3 server.
                                """)
        private String region = "";

        @ConfigProperty(
                defaultValue = "5",
                description =
                        """
                                Time in seconds to sleep after polling for new files.
                                """)
        @JsonProperty("idle-time")
        private int idleTime;

        @ConfigProperty(
                defaultValue = DEFAULT_FILE_EXTENSIONS,
                description =
                        """
                                Comma separated list of file extensions to filter by.
                                """)
        @JsonProperty("file-extensions")
        private String fileExtensions = DEFAULT_FILE_EXTENSIONS;
    }

    @AgentConfig(
            name = "Azure Blob Storage Source",
            description =
                    """
    Reads data from Azure blobs. There are three supported ways to authenticate:
    - SAS token
    - Storage account name and key
    - Storage account connection string
    """)
    @Data
    public static class AzureBlobStorageConfiguration {

        @ConfigProperty(
                defaultValue = "langstream-azure-source",
                description =
                        """
                                The name of the Azure econtainer to read from.
                                """)
        private String container;

        @ConfigProperty(
                required = true,
                description =
                        """
                                Endpoint to connect to. Usually it's https://<storage-account>.blob.core.windows.net.
                                """)
        private String endpoint;

        @ConfigProperty(
                description =
                        """
                        Azure SAS token. If not provided, storage account name and key must be provided.
                        """)
        @JsonProperty("sas-token")
        private String sasToken;

        @ConfigProperty(
                description =
                        """
                        Azure storage account name. If not provided, SAS token must be provided.
                        """)
        @JsonProperty("storage-account-name")
        private String storageAccountName;

        @ConfigProperty(
                description =
                        """
                        Azure storage account key. If not provided, SAS token must be provided.
                        """)
        @JsonProperty("storage-account-key")
        private String storageAccountKey;

        @ConfigProperty(
                description =
                        """
                        Azure storage account connection string. If not provided, SAS token must be provided.
                        """)
        @JsonProperty("storage-account-connection-string")
        private String storageAccountConnectionString;

        @ConfigProperty(
                defaultValue = "5",
                description =
                        """
                Time in seconds to sleep after polling for new files.
                                """)
        @JsonProperty("idle-time")
        private int idleTime;

        @ConfigProperty(
                defaultValue = "pdf,docx,html,htm,md,txt",
                description =
                        """
                                Comma separated list of file extensions to filter by.
                                """)
        @JsonProperty("file-extensions")
        private String fileExtensions;
    }
}
