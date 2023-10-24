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
package ai.langstream.runtime.api.agent;

public class AgentRunnerConstants {

    public static final String POD_CONFIG_ENV = "LANGSTREAM_AGENT_RUNNER_POD_CONFIGURATION";
    public static final String POD_CONFIG_ENV_DEFAULT = "/app-config/config";
    public static final String DOWNLOADED_CODE_PATH_ENV = "LANGSTREAM_AGENT_RUNNER_CODE_PATH";
    public static final String DOWNLOADED_CODE_PATH_ENV_DEFAULT = "/app-code-download";
    public static final String AGENTS_ENV = "LANGSTREAM_AGENT_RUNNER_AGENTS";
    public static final String AGENTS_ENV_DEFAULT = "/app/agents";
    public static final String PERSISTENT_VOLUMES_PATH =
            "LANGSTREAM_AGENT_RUNNER_PERSISTENT_VOLUMES_PATH";
    public static final String PERSISTENT_VOLUMES_PATH_DEFAULT = "/persistent-state";
}
