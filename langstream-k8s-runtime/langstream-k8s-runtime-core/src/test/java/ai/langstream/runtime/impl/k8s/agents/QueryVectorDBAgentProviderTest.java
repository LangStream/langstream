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

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueryVectorDBAgentProviderTest {

    @Test
    @SneakyThrows
    public void testValidationQueryDb() {
        validate(
                """
                pipeline:
                  - name: "db"
                    type: "query-vector-db"
                    configuration:
                      output-field: result
                      query: "select xxx"
                      datasource: "cassandra"
                      unknown-field: "..."
                """,
                "Found error on agent configuration (agent: 'db', type: 'query-vector-db'). Property 'unknown-field' is unknown");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "query-vector-db"
                    configuration:
                      output-field: result
                      query: "select xxx"
                      datasource: "not exists"
                """,
                "Resource 'not exists' not found");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "query-vector-db"
                    configuration:
                      output-field: result
                      query: "select xxx"
                      datasource: "cassandra"
                """,
                null);
    }

    @Test
    @SneakyThrows
    public void testWriteDb() {
        validate(
                """
                pipeline:
                  - name: "db"
                    type: "vector-db-sink"
                    configuration:
                      datasource: "not exists"
                      unknown-field: "..."
                """,
                "Resource 'not exists' not found");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "vector-db-sink"
                    configuration:
                      unknown-field: "..."
                """,
                "Found error on agent configuration (agent: 'db', type: 'vector-db-sink'). Property 'datasource' is required");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "vector-db-sink"
                    configuration:
                      datasource: "cassandra"
                      unknown-field: "..."
                      table-name: "my-table"
                      mapping: "..."
                """,
                null);
    }

    @Test
    @SneakyThrows
    public void testWritePinecone() {
        validate(
                """
                        pipeline:
                          - name: "db"
                            type: "vector-db-sink"
                            configuration:
                                datasource: "PineconeDatasource"
                                vector.id: "value.id"
                                vector.vector: "value.embeddings"
                                vector.namespace: "value.namespace"
                                vector.metadata.genre: "value.genre"
                                vector.metadata.genre2: "value.genre2"

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
                                "vector-db-sink",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                         {
                           "query-vector-db" : {
                             "name" : "Query a vector database",
                             "description" : "Query a vector database using Vector Search capabilities.",
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
                           "vector-db-sink_astra" : {
                             "type" : "vector-db-sink",
                             "name" : "Astra",
                             "description" : "Writes data to DataStax Astra service.\\nAll the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html",
                             "properties" : {
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'astra'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "keyspace" : {
                                 "description" : "The keyspace of the table to write to.",
                                 "required" : false,
                                 "type" : "string"
                               },
                               "mapping" : {
                                 "description" : "Comma separated list of mapping between the table column and the record field. e.g. my_colum_id=key, my_column_name=value.name.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "table-name" : {
                                 "description" : "The name of the table to write to. The table must already exist.",
                                 "required" : true,
                                 "type" : "string"
                               }
                             }
                           },
                           "vector-db-sink_astra-vector-db" : {
                             "type" : "vector-db-sink",
                             "name" : "Astra Vector DB",
                             "description" : "Writes data to Apache Cassandra.\\nAll the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html",
                             "properties" : {
                               "collection-name" : {
                                 "description" : "The name of the collection to write to. The collection must already exist.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'astra-vector-db'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "fields" : {
                                 "description" : "Fields of the collection to write to.",
                                 "required" : true,
                                 "type" : "array",
                                 "items" : {
                                   "description" : "Fields of the collection to write to.",
                                   "required" : true,
                                   "type" : "object",
                                   "properties" : {
                                     "expression" : {
                                       "description" : "JSTL Expression for computing the field value.",
                                       "required" : true,
                                       "type" : "string"
                                     },
                                     "name" : {
                                       "description" : "Field name",
                                       "required" : true,
                                       "type" : "string"
                                     }
                                   }
                                 }
                               }
                             }
                           },
                           "vector-db-sink_cassandra" : {
                             "type" : "vector-db-sink",
                             "name" : "Cassandra",
                             "description" : "Writes data to Apache Cassandra.\\nAll the options from DataStax Kafka Sink are supported: https://docs.datastax.com/en/kafka/doc/kafka/kafkaConfigTasksTOC.html",
                             "properties" : {
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'cassandra'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "keyspace" : {
                                 "description" : "The keyspace of the table to write to.",
                                 "required" : false,
                                 "type" : "string"
                               },
                               "mapping" : {
                                 "description" : "Comma separated list of mapping between the table column and the record field. e.g. my_colum_id=key, my_column_name=value.name.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "table-name" : {
                                 "description" : "The name of the table to write to. The table must already exist.",
                                 "required" : true,
                                 "type" : "string"
                               }
                             }
                           },
                           "vector-db-sink_jdbc" : {
                             "type" : "vector-db-sink",
                             "name" : "JDBC",
                             "description" : "Writes data to any JDBC compatible database.",
                             "properties" : {
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'jdbc'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "fields" : {
                                 "description" : "Fields of the table to write to.",
                                 "required" : true,
                                 "type" : "array",
                                 "items" : {
                                   "description" : "Fields of the table to write to.",
                                   "required" : true,
                                   "type" : "object",
                                   "properties" : {
                                     "expression" : {
                                       "description" : "JSTL Expression for computing the field value.",
                                       "required" : true,
                                       "type" : "string"
                                     },
                                     "name" : {
                                       "description" : "Field name",
                                       "required" : true,
                                       "type" : "string"
                                     },
                                     "primary-key" : {
                                       "description" : "Is this field part of the primary key?",
                                       "required" : false,
                                       "type" : "boolean",
                                       "defaultValue" : "false"
                                     }
                                   }
                                 }
                               },
                               "table-name" : {
                                 "description" : "The name of the table to write to. The table must already exist.",
                                 "required" : true,
                                 "type" : "string"
                               }
                             }
                           },
                           "vector-db-sink_milvus" : {
                             "type" : "vector-db-sink",
                             "name" : "Milvus",
                             "description" : "Writes data to Milvus/Zillis service.",
                             "properties" : {
                               "collection-name" : {
                                 "description" : "Collection name",
                                 "required" : false,
                                 "type" : "string"
                               },
                               "database-name" : {
                                 "description" : "Collection name",
                                 "required" : false,
                                 "type" : "string"
                               },
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'milvus'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "fields" : {
                                 "description" : "Fields definition.",
                                 "required" : true,
                                 "type" : "array",
                                 "items" : {
                                   "description" : "Fields definition.",
                                   "required" : true,
                                   "type" : "object",
                                   "properties" : {
                                     "expression" : {
                                       "description" : "JSTL Expression for computing the field value.",
                                       "required" : true,
                                       "type" : "string"
                                     },
                                     "name" : {
                                       "description" : "Field name",
                                       "required" : true,
                                       "type" : "string"
                                     }
                                   }
                                 }
                               }
                             }
                           },
                           "vector-db-sink_opensearch" : {
                             "type" : "vector-db-sink",
                             "name" : "OpenSearch",
                             "description" : "Writes data to OpenSearch or AWS OpenSearch serverless.",
                             "properties" : {
                               "batch-size" : {
                                 "description" : "Batch size for bulk operations. Hitting the batch size will trigger a flush.",
                                 "required" : false,
                                 "type" : "integer",
                                 "defaultValue" : "10"
                               },
                               "bulk-parameters" : {
                                 "description" : "OpenSearch bulk URL parameters.",
                                 "required" : false,
                                 "type" : "object",
                                 "properties" : {
                                   "pipeline" : {
                                     "description" : "The pipeline ID for preprocessing documents.\\nRefer to the OpenSearch documentation for more details.",
                                     "required" : false,
                                     "type" : "string"
                                   },
                                   "refresh" : {
                                     "description" : "Whether to refresh the affected shards after performing the indexing operations. Default is false. true makes the changes show up in search results immediately, but hurts cluster performance. wait_for waits for a refresh. Requests take longer to return, but cluster performance doesn’t suffer.\\nNote that AWS OpenSearch supports only false.\\nRefer to the OpenSearch documentation for more details.",
                                     "required" : false,
                                     "type" : "string"
                                   },
                                   "require_alias" : {
                                     "description" : "Set to true to require that all actions target an index alias rather than an index.\\nRefer to the OpenSearch documentation for more details.",
                                     "required" : false,
                                     "type" : "boolean"
                                   },
                                   "routing" : {
                                     "description" : "Routes the request to the specified shard.\\nRefer to the OpenSearch documentation for more details.",
                                     "required" : false,
                                     "type" : "string"
                                   },
                                   "timeout" : {
                                     "description" : "How long to wait for the request to return.\\nRefer to the OpenSearch documentation for more details.",
                                     "required" : false,
                                     "type" : "string"
                                   },
                                   "wait_for_active_shards" : {
                                     "description" : "Specifies the number of active shards that must be available before OpenSearch processes the bulk request. Default is 1 (only the primary shard). Set to all or a positive integer. Values greater than 1 require replicas. For example, if you specify a value of 3, the index must have two replicas distributed across two additional nodes for the request to succeed.\\nRefer to the OpenSearch documentation for more details.",
                                     "required" : false,
                                     "type" : "string"
                                   }
                                 }
                               },
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'opensearch'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "fields" : {
                                 "description" : "Index fields definition.",
                                 "required" : true,
                                 "type" : "array",
                                 "items" : {
                                   "description" : "Index fields definition.",
                                   "required" : true,
                                   "type" : "object",
                                   "properties" : {
                                     "expression" : {
                                       "description" : "JSTL Expression for computing the field value.",
                                       "required" : true,
                                       "type" : "string"
                                     },
                                     "name" : {
                                       "description" : "Field name",
                                       "required" : true,
                                       "type" : "string"
                                     }
                                   }
                                 }
                               },
                               "flush-interval" : {
                                 "description" : "Flush interval in milliseconds",
                                 "required" : false,
                                 "type" : "integer",
                                 "defaultValue" : "1000"
                               },
                               "id" : {
                                 "description" : "JSTL Expression to compute the index _id field. Leave it empty to let OpenSearch auto-generate the _id field.",
                                 "required" : false,
                                 "type" : "string"
                               }
                             }
                           },
                           "vector-db-sink_pinecone" : {
                             "type" : "vector-db-sink",
                             "name" : "Pinecone",
                             "description" : "Writes data to Pinecone service.\\n    To add metadata fields you can add vector.metadata.my-field: \\"value.my-field\\". The value is a JSTL Expression to compute the actual value.",
                             "properties" : {
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'pinecone'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "vector.id" : {
                                 "description" : "JSTL Expression to compute the id.",
                                 "required" : false,
                                 "type" : "string"
                               },
                               "vector.metadata" : {
                                 "description" : "Metadata to append. The key is the metadata name and the value the JSTL Expression to compute the actual value.",
                                 "required" : false,
                                 "type" : "object"
                               },
                               "vector.namespace" : {
                                 "description" : "JSTL Expression to compute the namespace.",
                                 "required" : false,
                                 "type" : "string"
                               },
                               "vector.vector" : {
                                 "description" : "JSTL Expression to compute the vector.",
                                 "required" : false,
                                 "type" : "string"
                               }
                             }
                           },
                           "vector-db-sink_solr" : {
                             "type" : "vector-db-sink",
                             "name" : "Apache Solr",
                             "description" : "Writes data to Apache Solr service.\\n    The collection-name is configured at datasource level.",
                             "properties" : {
                               "commit-within" : {
                                 "description" : "Commit within option",
                                 "required" : false,
                                 "type" : "integer",
                                 "defaultValue" : "1000"
                               },
                               "datasource" : {
                                 "description" : "Resource id. The target resource must be type: 'datasource' or 'vector-database' and service: 'solr'.",
                                 "required" : true,
                                 "type" : "string"
                               },
                               "fields" : {
                                 "description" : "Fields definition.",
                                 "required" : true,
                                 "type" : "array",
                                 "items" : {
                                   "description" : "Fields definition.",
                                   "required" : true,
                                   "type" : "object",
                                   "properties" : {
                                     "expression" : {
                                       "description" : "JSTL Expression for computing the field value.",
                                       "required" : true,
                                       "type" : "string"
                                     },
                                     "name" : {
                                       "description" : "Field name",
                                       "required" : true,
                                       "type" : "string"
                                     }
                                   }
                                 }
                               }
                             }
                           }
                         }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
