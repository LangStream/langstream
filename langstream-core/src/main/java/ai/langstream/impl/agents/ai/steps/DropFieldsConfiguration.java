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
package ai.langstream.impl.agents.ai.steps;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import java.util.List;
import lombok.Data;

@AgentConfig(
        name = "Drop fields",
        description = """
                Drops the record fields.
                """)
@Data
public class DropFieldsConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return DropFieldsConfiguration.class;
                }
            };

    @ConfigProperty(
            description =
                    """
                            Fields to drop from the input record.
                            """,
            required = true)
    private List<String> fields;

    @ConfigProperty(
            description =
                    """
                            Part to drop. (value or key)
                            """)
    private String part;
}
