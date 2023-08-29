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
package ai.langstream.impl.agents;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.impl.common.AbstractAgentProvider;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** Implements support for agents that can be composed into a single Composable Agent. */
@Slf4j
public abstract class AbstractComposableAgentProvider extends AbstractAgentProvider {

    public AbstractComposableAgentProvider(
            Set<String> supportedTypes, List<String> supportedRuntimes) {
        super(supportedTypes, supportedRuntimes);
    }

    @Override
    protected boolean isComposable(AgentConfiguration agentConfiguration) {
        return true;
    }
}
