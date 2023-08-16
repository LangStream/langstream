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
package com.datastax.oss.sga.api.runner.code;

import java.util.List;
import java.util.Map;

/**
 * Body of the agent
 */
public interface AgentCode {

    String agentId();

    /**
     * Get the type of the agent.
     * @return the type of the agent
     */
    String agentType();

    default void setMetadata(String id, String agentType, long startedAt) throws Exception {
    }
    default void init(Map<String, Object> configuration) throws Exception {
    }

    default void setContext(AgentContext context) throws Exception {
    }

    default void start() throws Exception {}
    default void close() throws Exception {}

    /**
     * Return information about the agent.
     * This is a List because an Agent can be the composition of multiple agents.
     * @return
     */
    List<AgentStatusResponse> getAgentStatus();
}
