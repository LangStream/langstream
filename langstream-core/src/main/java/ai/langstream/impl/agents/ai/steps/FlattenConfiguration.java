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
import lombok.Data;

@AgentConfig(
        name = "Flatten record fields",
        description =
                """
        Converts structured nested data into a new single-hierarchy-level structured data. The names of the new fields are built by concatenating the intermediate level field names.
        """)
@Data
public class FlattenConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return FlattenConfiguration.class;
                }
            };

    @ConfigProperty(
            description =
                    """
                            The delimiter to use when concatenating the field names.
                            """,
            defaultValue = "_")
    private String delimiter;

    @ConfigProperty(
            description =
                    """
                            When used with KeyValue data, defines if the transformation is done on the key or on the value. If empty, the transformation applies to both the key and the value.
                            """)
    private String part;
}
