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
package ai.langstream.agents.vector.pinecone;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public final class PineconeConfig {
    @JsonProperty(value = "api-key", required = true)
    private String apiKey;

    @JsonProperty(value = "environment", required = true)
    private String environment = "default";

    @JsonProperty(value = "project-name", required = true)
    private String projectName;

    @JsonProperty(value = "index-name", required = true)
    private String indexName;

    @JsonProperty(value = "endpoint")
    private String endpoint;

    @JsonProperty("server-side-timeout-sec")
    private int serverSideTimeoutSec = 10;
}
