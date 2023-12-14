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
package ai.langstream.runtime.impl.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KubernetesGenAIToolKitFunctionAgentProviderTest {

    @Test
    @SneakyThrows
    public void testValidationDropFields() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                      a-field: "val"
                """,
                "Found error on agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'a-field' is unknown");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration: {}
                """,
                "Found error on agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'fields' is required");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                        fields: {}
                """,
                "Found error on agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'fields' has a wrong data type. Expected type: java.util.ArrayList");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                      fields:
                      - "a"
                      part: value
                """,
                null);
    }

    @Test
    @SneakyThrows
    public void testValidationAiChatCompletions() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "chat"
                    type: "ai-chat-completions"
                    configuration:
                        model: my-model
                        messages:
                         - role: system
                           content: "Hello"
                """,
                "Found error on agent configuration (agent: 'chat', type: 'ai-chat-completions'). No ai service resource found in application configuration. One of vertex-configuration, hugging-face-configuration, open-ai-configuration, bedrock-configuration, ollama-configuration must be defined.");

        AgentValidationTestUtil.validate(
                """
                topics: []
                pipeline:
                  - name: "chat"
                    type: "ai-chat-completions"
                    configuration:
                        model: my-model
                        messages:
                         - role: system
                           content: "Hello"
                """,
                """
                        configuration:
                            resources:
                                - type: "open-ai-configuration"
                                  name: "OpenAI Azure configuration"
                                  configuration:
                                    access-key: "yy"
                        """,
                null);
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        AgentValidationTestUtil.validate(pipeline, expectErrMessage);
    }

    @Test
    @SneakyThrows
    public void testDocumentation() {
        final Map<String, AgentConfigurationModel> model =
                new PluginsRegistry()
                        .lookupAgentImplementation(
                                "drop-fields",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                          {
                            "ai-chat-completions" : {
                              "name" : "Compute chat completions",
                              "description" : "Sends the messages to the AI Service to compute chat completions. The result is stored in the specified field.",
                              "properties" : {
                                "ai-service" : {
                                  "description" : "In case of multiple AI services configured, specify the id of the AI service to use.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "completion-field" : {
                                  "description" : "Field to use to store the completion results in the output topic. Use \\"value\\" to write the result without a structured schema. Use \\"value.<field>\\" to write the result in a specific field.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "frequency-penalty" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "log-field" : {
                                  "description" : "Field to use to store the log of the completion results in the output topic. Use \\"value\\" to write the result without a structured schema. Use \\"value.<field>\\" to write the result in a specific field.\\nThe log contains useful information for debugging the completion prompts.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "logit-bias" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "object"
                                },
                                "max-tokens" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "integer"
                                },
                                "messages" : {
                                  "description" : "Messages to use for chat completions. You can use the Mustache syntax.",
                                  "required" : true,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "Messages to use for chat completions. You can use the Mustache syntax.",
                                    "required" : true,
                                    "type" : "object",
                                    "properties" : {
                                      "content" : {
                                        "description" : "Content of the message. You can use the Mustache syntax.",
                                        "required" : true,
                                        "type" : "string"
                                      },
                                      "role" : {
                                        "description" : "Role of the message. The role is used to identify the speaker in the chat.",
                                        "required" : false,
                                        "type" : "string"
                                      }
                                    }
                                  }
                                },
                                "min-chunks-per-message" : {
                                  "description" : "Minimum number of chunks to send to the stream-to-topic topic. The chunks are sent as soon as they are available.\\nThe chunks are sent in the order they are received from the AI Service.\\nTo improve the TTFB (Time-To-First-Byte), the chunk size starts from 1 and doubles until it reaches the max-chunks-per-message value.",
                                  "required" : false,
                                  "type" : "integer",
                                  "defaultValue" : "20"
                                },
                                "model" : {
                                  "description" : "The model to use for chat completions. The model must be available in the AI Service.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "options" : {
                                  "description" : "Additional options for the model configuration. The structure depends on the model and AI provider.",
                                  "required" : false,
                                  "type" : "object"
                                },
                                "presence-penalty" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "stop" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                    "required" : false,
                                    "type" : "string"
                                  }
                                },
                                "stream" : {
                                  "description" : "Enable streaming of the results. Use in conjunction with the stream-to-topic parameter.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "stream-response-completion-field" : {
                                  "description" : "Field to use to store the completion results in the stream-to-topic topic. Use \\"value\\" to write the result without a structured schema. Use \\"value.<field>\\" to write the result in a specific field.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "stream-to-topic" : {
                                  "description" : "Enable streaming of the results. If enabled, the results are streamed to the specified topic in small chunks. The entire messages will be sent to the output topic instead.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "temperature" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "top-p" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "user" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "ai-text-completions" : {
                              "name" : "Compute text completions",
                              "description" : "Sends the text to the AI Service to compute text completions. The result is stored in the specified field.",
                              "properties" : {
                                "ai-service" : {
                                  "description" : "In case of multiple AI services configured, specify the id of the AI service to use.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "completion-field" : {
                                  "description" : "Field to use to store the completion results in the output topic. Use \\"value\\" to write the result without a structured schema. Use \\"value.<field>\\" to write the result in a specific field.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "frequency-penalty" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "log-field" : {
                                  "description" : "Field to use to store the log of the completion results in the output topic. Use \\"value\\" to write the result without a structured schema. Use \\"value.<field>\\" to write the result in a specific field.\\nThe log contains useful information for debugging the completion prompts.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "logit-bias" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "object"
                                },
                                "logprobs" : {
                                  "description" : "Logprobs parameter (only valid for OpenAI).",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "logprobs-field" : {
                                  "description" : "Log probabilities to a field.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "max-tokens" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "integer"
                                },
                                "min-chunks-per-message" : {
                                  "description" : "Minimum number of chunks to send to the stream-to-topic topic. The chunks are sent as soon as they are available.\\nThe chunks are sent in the order they are received from the AI Service.\\nTo improve the TTFB (Time-To-First-Byte), the chunk size starts from 1 and doubles until it reaches the max-chunks-per-message value.",
                                  "required" : false,
                                  "type" : "integer",
                                  "defaultValue" : "20"
                                },
                                "model" : {
                                  "description" : "The model to use for text completions. The model must be available in the AI Service.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "options" : {
                                  "description" : "Additional options for the model configuration. The structure depends on the model and AI provider.",
                                  "required" : false,
                                  "type" : "object"
                                },
                                "presence-penalty" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "prompt" : {
                                  "description" : "Prompt to use for text completions. You can use the Mustache syntax.",
                                  "required" : true,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "Prompt to use for text completions. You can use the Mustache syntax.",
                                    "required" : true,
                                    "type" : "string"
                                  }
                                },
                                "stop" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                    "required" : false,
                                    "type" : "string"
                                  }
                                },
                                "stream" : {
                                  "description" : "Enable streaming of the results. Use in conjunction with the stream-to-topic parameter.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "stream-response-completion-field" : {
                                  "description" : "Field to use to store the completion results in the stream-to-topic topic. Use \\"value\\" to write the result without a structured schema. Use \\"value.<field>\\" to write the result in a specific field.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "stream-to-topic" : {
                                  "description" : "Enable streaming of the results. If enabled, the results are streamed to the specified topic in small chunks. The entire messages will be sent to the output topic instead.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "temperature" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "top-p" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "number"
                                },
                                "user" : {
                                  "description" : "Parameter for the completion request. The parameters are passed to the AI Service as is.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "cast" : {
                              "name" : "Cast record to another schema",
                              "description" : "Transforms the data to a target compatible schema.\\nSome step operations like cast or compute involve conversions from a type to another. When this happens the rules are:\\n    - timestamp, date and time related object conversions assume UTC time zone if it is not explicit.\\n    - date and time related object conversions to/from STRING use the RFC3339 format.\\n    - timestamp related object conversions to/from LONG and DOUBLE are done using the number of milliseconds since EPOCH (1970-01-01T00:00:00Z).\\n    - date related object conversions to/from INTEGER, LONG, FLOAT and DOUBLE are done using the number of days since EPOCH (1970-01-01).\\n    - time related object conversions to/from INTEGER, LONG and DOUBLE are done using the number of milliseconds since midnight (00:00:00).",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "part" : {
                                  "description" : "When used with KeyValue data, defines if the transformation is done on the key or on the value. If empty, the transformation applies to both the key and the value.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "schema-type" : {
                                  "description" : "The target schema type.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "compute" : {
                              "name" : "Compute values from the record",
                              "description" : "Computes new properties, values or field values based on an expression evaluated at runtime. If the field already exists, it will be overwritten.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "fields" : {
                                  "description" : "An array of objects describing how to calculate the field values",
                                  "required" : true,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "An array of objects describing how to calculate the field values",
                                    "required" : true,
                                    "type" : "object",
                                    "properties" : {
                                      "expression" : {
                                        "description" : "It is evaluated at runtime and the result of the evaluation is assigned to the field.\\nRefer to the language expression documentation for more information on the expression syntax.",
                                        "required" : true,
                                        "type" : "string",
                                        "extendedValidationType" : "EL_EXPRESSION"
                                      },
                                      "name" : {
                                        "description" : "The name of the field to be computed. Prefix with key. or value. to compute the fields in the key or value parts of the message.\\nIn addition, you can compute values on the following message headers [destinationTopic, messageKey, properties.].\\nPlease note that properties is a map of key/value pairs that are referenced by the dot notation, for example properties.key0.",
                                        "required" : true,
                                        "type" : "string"
                                      },
                                      "optional" : {
                                        "description" : "If true, it marks the field as optional in the schema of the transformed message. This is useful when null is a possible value of the compute expression.",
                                        "required" : false,
                                        "type" : "boolean",
                                        "defaultValue" : "false"
                                      },
                                      "type" : {
                                        "description" : "The type of the computed field. This\\n will translate to the schema type of the new field in the transformed message.\\n The following types are currently supported :STRING, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, DATE, TIME, TIMESTAMP, LOCAL_DATE_TIME, LOCAL_TIME, LOCAL_DATE, INSTANT.\\n  The type field is not required for the message headers [destinationTopic, messageKey, properties.] and STRING will be used.\\n  For the value and key, if it is not provided, then the type will be inferred from the result of the expression evaluation.",
                                        "required" : false,
                                        "type" : "string"
                                      }
                                    }
                                  }
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "compute-ai-embeddings" : {
                              "name" : "Compute embeddings of the record",
                              "description" : "Compute embeddings of the record. The embeddings are stored in the record under a specific field.",
                              "properties" : {
                                "ai-service" : {
                                  "description" : "In case of multiple AI services configured, specify the id of the AI service to use.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "arguments" : {
                                  "description" : "Additional arguments to pass to the AI Service. (HuggingFace only)",
                                  "required" : false,
                                  "type" : "object"
                                },
                                "batch-size" : {
                                  "description" : "Batch size for submitting the embeddings requests.",
                                  "required" : false,
                                  "type" : "integer",
                                  "defaultValue" : "10"
                                },
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "concurrency" : {
                                  "description" : "Max number of concurrent requests to the AI Service.",
                                  "required" : false,
                                  "type" : "integer",
                                  "defaultValue" : "4"
                                },
                                "embeddings-field" : {
                                  "description" : "Field where to store the embeddings.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "flush-interval" : {
                                  "description" : "Flushing is disabled by default in order to avoid latency spikes.\\nYou should enable this feature in the case of background processing.",
                                  "required" : false,
                                  "type" : "integer",
                                  "defaultValue" : "0"
                                },
                                "loop-over" : {
                                  "description" : "Execute the agent over a list of documents",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "model" : {
                                  "description" : "Model to use for the embeddings. The model must be available in the configured AI Service.",
                                  "required" : false,
                                  "type" : "string",
                                  "defaultValue" : "text-embedding-ada-002"
                                },
                                "model-url" : {
                                  "description" : "URL of the model to use. (HuggingFace only). The default is computed from the model: \\"djl://ai.djl.huggingface.pytorch/{model}\\"",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "options" : {
                                  "description" : "Additional options to pass to the AI Service. (HuggingFace only)",
                                  "required" : false,
                                  "type" : "object"
                                },
                                "text" : {
                                  "description" : "Text to create embeddings from. You can use Mustache syntax to compose multiple fields into a single text. Example:\\ntext: \\"{{{ value.field1 }}} {{{ value.field2 }}}\\"",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "drop" : {
                              "name" : "Drop the record",
                              "description" : "Drops the record from further processing. Use in conjunction with when to selectively drop records.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "drop-fields" : {
                              "name" : "Drop fields",
                              "description" : "Drops the record fields.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "fields" : {
                                  "description" : "Fields to drop from the input record.",
                                  "required" : true,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "Fields to drop from the input record.",
                                    "required" : true,
                                    "type" : "string"
                                  }
                                },
                                "part" : {
                                  "description" : "Part to drop. (value or key)",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "flatten" : {
                              "name" : "Flatten record fields",
                              "description" : "Converts structured nested data into a new single-hierarchy-level structured data. The names of the new fields are built by concatenating the intermediate level field names.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "delimiter" : {
                                  "description" : "The delimiter to use when concatenating the field names.",
                                  "required" : false,
                                  "type" : "string",
                                  "defaultValue" : "_"
                                },
                                "part" : {
                                  "description" : "When used with KeyValue data, defines if the transformation is done on the key or on the value. If empty, the transformation applies to both the key and the value.",
                                  "required" : false,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "merge-key-value" : {
                              "name" : "Merge key-value format",
                              "description" : "Merges the fields of KeyValue records where both the key and value are structured types of the same schema type. Only AVRO and JSON are supported.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "query" : {
                              "name" : "Query",
                              "description" : "Perform a vector search or simple query against a datasource.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "datasource" : {
                                  "description" : "Reference to a datasource id configured in the application.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "fields" : {
                                  "description" : "Fields of the record to use as input parameters for the query.",
                                  "required" : false,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "Fields of the record to use as input parameters for the query.",
                                    "required" : false,
                                    "type" : "string",
                                    "extendedValidationType" : "EL_EXPRESSION"
                                  },
                                  "extendedValidationType" : "EL_EXPRESSION"
                                },
                                "generated-keys" : {
                                  "description" : "List of fields to use as generated keys. The generated keys are returned in the output, depending on the database.",
                                  "required" : false,
                                  "type" : "array",
                                  "items" : {
                                    "description" : "List of fields to use as generated keys. The generated keys are returned in the output, depending on the database.",
                                    "required" : false,
                                    "type" : "string"
                                  }
                                },
                                "loop-over" : {
                                  "description" : "Loop over a list of items taken from the record. For instance value.documents.\\nIt must refer to a list of maps. In this case the output-field parameter\\nbut be like \\"record.fieldname\\" in order to replace or set a field in each record\\nwith the results of the query. In the query parameters you can refer to the\\nrecord fields using \\"record.field\\".",
                                  "required" : false,
                                  "type" : "string",
                                  "extendedValidationType" : "EL_EXPRESSION"
                                },
                                "mode" : {
                                  "description" : "Execution mode: query or execute. In query mode, the query is executed and the results are returned. In execute mode, the query is executed and the result is the number of rows affected (depending on the database).",
                                  "required" : false,
                                  "type" : "string",
                                  "defaultValue" : "query"
                                },
                                "only-first" : {
                                  "description" : "If true, only the first result of the query is stored in the output field.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "false"
                                },
                                "output-field" : {
                                  "description" : "The name of the field to use to store the query result.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "query" : {
                                  "description" : "The query to use to extract the data.",
                                  "required" : true,
                                  "type" : "string"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            },
                            "unwrap-key-value" : {
                              "name" : "Unwrap key-value format",
                              "description" : "If the record value is in KeyValue format, extracts the KeyValue's key or value and make it the record value.",
                              "properties" : {
                                "composable" : {
                                  "description" : "Whether this step can be composed with other steps.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "true"
                                },
                                "unwrapKey" : {
                                  "description" : "Whether to unwrap the key instead of the value.",
                                  "required" : false,
                                  "type" : "boolean",
                                  "defaultValue" : "false"
                                },
                                "when" : {
                                  "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                  "required" : false,
                                  "type" : "string"
                                }
                              }
                            }
                          }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
