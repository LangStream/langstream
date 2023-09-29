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
package ai.langstream.agents.s3;

import ai.langstream.api.doc.AgentConfiguration;
import ai.langstream.api.doc.ConfigProperty;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@AgentConfiguration(name = "S3 Source", description = "Reads data from S3 bucket")
@Data
public class S3SourceConfiguration {

    protected static final String DEFAULT_BUCKET_NAME = "langstream-source";
    protected static final String DEFAULT_ENDPOINT = "http://minio-endpoint.-not-set:9090";
    protected static final String DEFAULT_ACCESSKEY = "minioadmin";
    protected static final String DEFAULT_SECRETKEY = "minioadmin";
    protected static final String DEFAULT_FILE_EXTENSIONS = "pdf,docx,html,htm,md,txt";

    @ConfigProperty(
            description = """
            The name of the bucket to read from.
            """,
            defaultValue = DEFAULT_BUCKET_NAME)
    private String bucketName = DEFAULT_BUCKET_NAME;

    @ConfigProperty(
            description = """
            The endpoint of the S3 server.
            """,
            defaultValue = DEFAULT_ENDPOINT)
    private String endpoint = DEFAULT_ENDPOINT;

    @ConfigProperty(
            description = """
            Access key for the S3 server.
            """,
            defaultValue = DEFAULT_ACCESSKEY)
    @JsonProperty("access-key")
    private String accessKey = DEFAULT_ACCESSKEY;

    @ConfigProperty(
            description = """
            Secret key for the S3 server.
            """,
            defaultValue = DEFAULT_SECRETKEY)
    @JsonProperty("secret-key")
    private String secretKey = DEFAULT_SECRETKEY;

    @ConfigProperty(
            required = false,
            description = """
                    Region for the S3 server.
                    """)
    private String region = "";

    @ConfigProperty(
            defaultValue = "5",
            description = """
                    Region for the S3 server.
                    """)
    @JsonProperty("idle-time")
    private int idleTime = 5;

    @ConfigProperty(
            defaultValue = DEFAULT_FILE_EXTENSIONS,
            description =
                    """
                    Comma separated list of file extensions to filter by.
                    """)
    @JsonProperty("file-extensions")
    private String fileExtensions = DEFAULT_FILE_EXTENSIONS;
}
