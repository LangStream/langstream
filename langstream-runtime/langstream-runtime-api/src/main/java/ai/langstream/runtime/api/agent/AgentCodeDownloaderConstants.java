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
package ai.langstream.runtime.api.agent;

public class AgentCodeDownloaderConstants {
    public static final String CLUSTER_CONFIG_ENV = "LANGSTREAM_AGENT_CODE_DOWNLOADER_CLUSTER_CONFIGURATION";
    public static final String CLUSTER_CONFIG_ENV_DEFAULT = "/cluster-config/config";
    public static final String TOKEN_ENV = "LANGSTREAM_AGENT_CODE_DOWNLOADER_TOKEN";
    public static final String TOKEN_ENV_DEFAULT = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    public static final String DOWNLOAD_CONFIG_ENV = "LANGSTREAM_AGENT_CODE_DOWNLOADER_DOWNLOAD_CONFIGURATION";
    public static final String DOWNLOAD_CONFIG_ENV_DEFAULT = "/download-config/config";
}
