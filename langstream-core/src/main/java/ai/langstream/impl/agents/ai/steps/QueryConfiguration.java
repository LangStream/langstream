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
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Data;

@AgentConfig(
        name = "Query",
        description =
                """
        Perform a vector search or simple query against a datasource.
        """)
@Data
public class QueryConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return QueryConfiguration.class;
                }

                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        GenAIToolKitFunctionAgentProvider.DataSourceConfigurationGenerator
                                dataSourceConfigurationGenerator,
                        GenAIToolKitFunctionAgentProvider.TopicConfigurationGenerator
                                topicConfigurationGenerator,
                        GenAIToolKitFunctionAgentProvider.AIServiceConfigurationGenerator
                                aiServiceConfigurationGenerator) {
                    GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer.super
                            .generateSteps(
                                    step,
                                    originalConfiguration,
                                    agentConfiguration,
                                    dataSourceConfigurationGenerator,
                                    topicConfigurationGenerator,
                                    aiServiceConfigurationGenerator);
                    String datasource = (String) step.remove("datasource");
                    if (datasource == null) {
                        throw new IllegalStateException(
                                "datasource is required but this exception should have been raised before ?");
                    }
                    dataSourceConfigurationGenerator.generateDataSourceConfiguration(datasource);
                }
            };

    @ConfigProperty(
            description =
                    """
                   The query to use to extract the data.
                   """,
            required = true)
    private String query;

    @ConfigProperty(
            description =
                    """
                   Loop over a list of items taken from the record. For instance value.documents.
                   It must refer to a list of maps. In this case the output-field parameter
                   but be like "record.fieldname" in order to replace or set a field in each record
                   with the results of the query. In the query parameters you can refer to the
                   record fields using "record.field".
                   """,
            extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
    @JsonProperty("loop-over")
    private String loopOver;

    @ConfigProperty(
            description =
                    """
                   Fields of the record to use as input parameters for the query.
                   """,
            extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
    private List<String> fields;

    @ConfigProperty(
            description =
                    """
                   The name of the field to use to store the query result.
                   """,
            required = true)
    @JsonProperty("output-field")
    private String outputField;

    @ConfigProperty(
            description =
                    """
                     If true, only the first result of the query is stored in the output field.
                   """,
            defaultValue = "false")
    @JsonProperty("only-first")
    private boolean onlyFirst;

    @ConfigProperty(
            description =
                    """
                   Reference to a datasource id configured in the application.
                   """,
            required = true)
    private String datasource;

    @ConfigProperty(
            description =
                    """
                   Execution mode: query or execute. In query mode, the query is executed and the results are returned. In execute mode, the query is executed and the result is the number of rows affected (depending on the database).
                   """,
            defaultValue = "query")
    private Mode mode = Mode.query;

    @ConfigProperty(
            description =
                    """
                   List of fields to use as generated keys. The generated keys are returned in the output, depending on the database.
                   """)
    @JsonProperty("generated-keys")
    private List<String> generatedKeys;

    enum Mode {
        query,
        execute
    }
}
