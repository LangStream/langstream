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
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@AgentConfig(
        name = "Cast record to another schema",
        description =
                """
        Transforms the data to a target compatible schema.
        Some step operations like cast or compute involve conversions from a type to another. When this happens the rules are:
            - timestamp, date and time related object conversions assume UTC time zone if it is not explicit.
            - date and time related object conversions to/from STRING use the RFC3339 format.
            - timestamp related object conversions to/from LONG and DOUBLE are done using the number of milliseconds since EPOCH (1970-01-01T00:00:00Z).
            - date related object conversions to/from INTEGER, LONG, FLOAT and DOUBLE are done using the number of days since EPOCH (1970-01-01).
            - time related object conversions to/from INTEGER, LONG and DOUBLE are done using the number of milliseconds since midnight (00:00:00).
        """)
@Data
public class CastConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return CastConfiguration.class;
                }
            };

    @ConfigProperty(
            description =
                    """
                            The target schema type.
                            """,
            required = true)
    @JsonProperty("schema-type")
    private String schemaType;

    @ConfigProperty(
            description =
                    """
                            When used with KeyValue data, defines if the transformation is done on the key or on the value. If empty, the transformation applies to both the key and the value.
                            """)
    private String part;
}
