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
package ai.langstream.api.runner.code;

import ai.langstream.api.runtime.ComponentType;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Body of the agent */
public interface AgentCode extends AutoCloseable {

    static final Logger log = LoggerFactory.getLogger(AgentCode.class);

    String agentId();

    /**
     * Get the type of the agent.
     *
     * @return the type of the agent
     */
    String agentType();

    ComponentType componentType();

    default void setMetadata(String id, String agentType, long startedAt) throws Exception {}

    default void init(Map<String, Object> configuration) throws Exception {}

    /**
     * Set the context of the agent. This is invoked after {@link #init(Map)} and before {@link
     * #start()}
     *
     * @param context the context of the agent
     * @throws Exception if an error occurs
     */
    default void setContext(AgentContext context) throws Exception {}

    default void start() throws Exception {}

    @Override
    default void close() throws Exception {}

    /**
     * Return information about the agent. This is a List because an Agent can be the composition of
     * multiple agents.
     *
     * @return information about the agent
     */
    List<AgentStatusResponse> getAgentStatus();

    /**
     * Gracefully restart the agent.
     *
     * @throws Exception if an error occurs
     */
    default void restart() throws Exception {}
}
