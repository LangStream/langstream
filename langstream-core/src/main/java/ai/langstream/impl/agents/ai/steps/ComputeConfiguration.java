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
import ai.langstream.api.doc.ExtendedValidationType;
import ai.langstream.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import java.util.List;
import lombok.Data;

@AgentConfig(
        name = "Compute values from the record",
        description =
                """
        Computes new properties, values or field values based on an expression evaluated at runtime. If the field already exists, it will be overwritten.
        """)
@Data
public class ComputeConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return ComputeConfiguration.class;
                }
            };

    @Data
    public static class Field {
        @ConfigProperty(
                description =
                        """
                                The name of the field to be computed. Prefix with key. or value. to compute the fields in the key or value parts of the message.
                                In addition, you can compute values on the following message headers [destinationTopic, messageKey, properties.].
                                Please note that properties is a map of key/value pairs that are referenced by the dot notation, for example properties.key0.""",
                required = true)
        private String name;

        @ConfigProperty(
                description =
                        """
                                It is evaluated at runtime and the result of the evaluation is assigned to the field.
                                Refer to the language expression documentation for more information on the expression syntax.
                                """,
                required = true,
                extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        private String expression;

        @ConfigProperty(
                description =
                        """
                                The type of the computed field. This
                                 will translate to the schema type of the new field in the transformed message.
                                 The following types are currently supported :STRING, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, DATE, TIME, TIMESTAMP, LOCAL_DATE_TIME, LOCAL_TIME, LOCAL_DATE, INSTANT.
                                  The type field is not required for the message headers [destinationTopic, messageKey, properties.] and STRING will be used.
                                  For the value and key, if it is not provided, then the type will be inferred from the result of the expression evaluation.
                                """,
                required = false)
        private String type;

        @ConfigProperty(
                description =
                        """
                                If true, it marks the field as optional in the schema of the transformed message. This is useful when null is a possible value of the compute expression.
                                """,
                defaultValue = "false")
        private boolean optional;
    }

    @ConfigProperty(
            description =
                    """
                            An array of objects describing how to calculate the field values
                            """,
            required = true)
    private List<Field> fields;
}
