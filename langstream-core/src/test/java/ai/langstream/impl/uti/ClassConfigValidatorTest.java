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
package ai.langstream.impl.uti;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ConfigPropertyIgnore;
import ai.langstream.api.doc.ExtendedValidationType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClassConfigValidatorTest {

    @Data
    @AgentConfig(description = "this agent is AWESOME!", name = "my super agent")
    public static class MyClass {
        @JsonProperty("my-int")
        @JsonPropertyDescription("this must be ignored")
        @ConfigProperty(
                required = true,
                description =
                        """
                                    my description
                                """,
                defaultValue = "11")
        private int myInt = 10;

        @ConfigProperty(required = true)
        private List<MyClass2> theList;

        @ConfigProperty(extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        private String expression;

        @ConfigProperty(required = true)
        private Map<String, MyClass2> theMap;

        MyClass2 myclass2;
        @ConfigPropertyIgnore Object ignoreMe;

        @JsonIgnore Object doNotIgnoreMe;
    }

    @Data
    public static class MyClass2 {
        private String inner;

        @ConfigProperty(required = true, description = "my description", defaultValue = "110.1a")
        private String innerDoc;
    }

    @Test
    public void testParseClass() throws Exception {

        final AgentConfigurationModel model =
                ClassConfigValidator.generateAgentModelFromClass(MyClass.class);

        Assertions.assertEquals(
                """
                              {
                                "name" : "my super agent",
                                "description" : "this agent is AWESOME!",
                                "properties" : {
                                  "doNotIgnoreMe" : {
                                    "required" : false
                                  },
                                  "expression" : {
                                    "required" : false,
                                    "type" : "string",
                                    "extendedValidationType" : "EL_EXPRESSION"
                                  },
                                  "my-int" : {
                                    "description" : "my description",
                                    "required" : true,
                                    "type" : "integer",
                                    "defaultValue" : "11"
                                  },
                                  "myclass2" : {
                                    "required" : false,
                                    "type" : "object",
                                    "properties" : {
                                      "innerDoc" : {
                                        "description" : "my description",
                                        "required" : true,
                                        "type" : "string",
                                        "defaultValue" : "110.1a"
                                      },
                                      "inner" : {
                                        "required" : false,
                                        "type" : "string"
                                      }
                                    }
                                  },
                                  "theList" : {
                                    "required" : true,
                                    "type" : "array",
                                    "items" : {
                                      "required" : true,
                                      "type" : "object",
                                      "properties" : {
                                        "innerDoc" : {
                                          "description" : "my description",
                                          "required" : true,
                                          "type" : "string",
                                          "defaultValue" : "110.1a"
                                        },
                                        "inner" : {
                                          "required" : false,
                                          "type" : "string"
                                        }
                                      }
                                    }
                                  },
                                  "theMap" : {
                                    "required" : true,
                                    "type" : "object"
                                  }
                                }
                              }""",
                new ObjectMapper()
                        .configure(SerializationFeature.INDENT_OUTPUT, true)
                        .setSerializationInclusion(
                                com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                        .writeValueAsString(model));
    }
}
