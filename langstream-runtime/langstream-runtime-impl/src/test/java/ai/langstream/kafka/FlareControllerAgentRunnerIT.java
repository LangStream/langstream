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
package ai.langstream.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@WireMockTest
class FlareControllerAgentRunnerIT extends AbstractKafkaApplicationRunner {

    @Container
    static GenericContainer database =
            new GenericContainer(DockerImageName.parse("herddb/herddb:0.28.0"))
                    .withExposedPorts(7000);

    @BeforeAll
    public static void startDatabase() {
        database.start();
    }

    @AfterAll
    public static void stopDatabase() {
        database.stop();
    }

    @Test
    public void testSimpleFlare(WireMockRuntimeInfo wireMockRuntimeInfo) throws Exception {

        String embeddingFirst = "[1.0,5.4,8.7,7,9]";
        stubFor(
                post("/openai/deployments/text-embeddings-ada/embeddings?api-version=2023-08-01-preview")
                        .willReturn(
                                okJson(
                                        """
                                                   {
                                                       "data": [
                                                         {
                                                           "embedding": %s,
                                                           "index": 0,
                                                           "object": "embedding"
                                                         }
                                                       ],
                                                       "model": "text-embedding-ada-002",
                                                       "object": "list",
                                                       "usage": {
                                                         "prompt_tokens": 5,
                                                         "total_tokens": 5
                                                       }
                                                     }
                                                """
                                                .formatted(embeddingFirst))));
        stubFor(
                post("/openai/deployments/gp-3.5-turbo-instruct/completions?api-version=2023-08-01-preview")
                        .willReturn(
                                okJson(
                                        """

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":"\\n\\n","index":0,"logprobs":{"tokens":["\\n\\n"],"token_logprobs":[-0.16865084],"top_logprobs":[{"\\n\\n":-0.16865084,"\\n":-1.9655257," \\n\\n":-5.4967756," \\n":-6.278025,"It":-6.465525}],"text_offset":[36]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":"I","index":0,"logprobs":{"tokens":["I"],"token_logprobs":[-0.50947005],"top_logprobs":[{"I":-0.50947005,"Without":-1.69697,"Unfortunately":-2.556345,"As":-3.2907197,"The":-3.2907197}],"text_offset":[38]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" am","index":0,"logprobs":{"tokens":[" am"],"token_logprobs":[-0.81064594],"top_logprobs":[{" am":-0.81064594,"'m":-1.2325209," cannot":-2.3106458," do":-2.701271," apologize":-3.2637708}],"text_offset":[39]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" an","index":0,"logprobs":{"tokens":[" an"],"token_logprobs":[-0.0639758],"top_logprobs":[{" an":-0.0639758," sorry":-3.5014758," a":-3.8608508," not":-5.001476," unable":-5.907726}],"text_offset":[42]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" AI","index":0,"logprobs":{"tokens":[" AI"],"token_logprobs":[-0.007819127],"top_logprobs":[{" AI":-0.007819127," artificial":-4.929694," Artificial":-8.476568," A":-9.304694," digital":-10.007819}],"text_offset":[45]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" language","index":0,"logprobs":{"tokens":[" language"],"token_logprobs":[-4.2176867],"top_logprobs":[{" language":-4.2176867," and":-0.03018677,",":-4.8114367," program":-5.983311," so":-6.6083117}],"text_offset":[48]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" model","index":0,"logprobs":{"tokens":[" model"],"token_logprobs":[-0.00009771052],"top_logprobs":[{" model":-0.00009771052," processing":-10.531347," AI":-10.578222," program":-11.875096," generation":-12.046971}],"text_offset":[57]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" and","index":0,"logprobs":{"tokens":[" and"],"token_logprobs":[-0.38906613],"top_logprobs":[{" and":-0.38906613,",":-1.2171912," so":-3.982816," trained":-5.9359407," created":-6.3421907}],"text_offset":[63]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" I","index":0,"logprobs":{"tokens":[" I"],"token_logprobs":[-1.1028589],"top_logprobs":[{" I":-1.1028589," do":-0.52473384," cannot":-3.1497335," don":-4.040359," therefore":-5.6653585}],"text_offset":[67]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" do","index":0,"logprobs":{"tokens":[" do"],"token_logprobs":[-0.18535662],"top_logprobs":[{" do":-0.18535662," don":-2.2478566," cannot":-3.1541064," am":-4.2791066," can":-5.482231}],"text_offset":[69]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" not","index":0,"logprobs":{"tokens":[" not"],"token_logprobs":[-0.00009115311],"top_logprobs":[{" not":-0.00009115311," no":-10.093841," n":-11.328216," have":-11.359466," ":-11.421966}],"text_offset":[72]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" have","index":0,"logprobs":{"tokens":[" have"],"token_logprobs":[-0.0122308275],"top_logprobs":[{" have":-0.0122308275," possess":-4.496606," know":-7.418481," physically":-8.746605," personally":-8.949731}],"text_offset":[76]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" personal","index":0,"logprobs":{"tokens":[" personal"],"token_logprobs":[-0.9290634],"top_logprobs":[{" personal":-0.9290634," access":-0.96031344," the":-2.1790633," any":-3.2571883," information":-3.929063}],"text_offset":[81]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" experiences","index":0,"logprobs":{"tokens":[" experiences"],"token_logprobs":[-0.2772571],"top_logprobs":[{" experiences":-0.2772571," experience":-2.121007," knowledge":-2.152257," or":-6.074132," information":-6.449132}],"text_offset":[90]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" or","index":0,"logprobs":{"tokens":[" or"],"token_logprobs":[-0.06607247],"top_logprobs":[{" or":-0.06607247,",":-3.3004472," with":-4.144197,".":-5.4566975," like":-5.862947}],"text_offset":[102]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" the","index":0,"logprobs":{"tokens":[" the"],"token_logprobs":[-2.1178281],"top_logprobs":[{" the":-2.1178281," knowledge":-0.74282813," information":-2.680328," senses":-2.9459531," opinions":-2.9615781}],"text_offset":[105]},"finish_reason":"length"}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":"","index":0,"logprobs":{"tokens":[],"token_logprobs":[],"top_logprobs":[],"text_offset":[]},"finish_reason":"length"}],"model":"gpt-3.5-turbo-instruct"}

                                  data: [DONE]
                                  """)));
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String jdbcUrl = "jdbc:herddb:server:localhost:" + database.getMappedPort(7000);

        Map<String, String> applicationWriter =
                Map.of(
                        "configuration.yaml",
                        """
                        configuration:
                          resources:
                            - type: "datasource"
                              name: "JdbcDatasource"
                              configuration:
                                service: "jdbc"
                                driverClass: "herddb.jdbc.Driver"
                                url: "%s"
                                user: "sa"
                                password: "hdb"
                                """
                                .formatted(jdbcUrl),
                        "module.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "jdbc-table"
                                    creation-mode: create-if-not-exists
                                    config:
                                      table-name: "documents"
                                      datasource: "JdbcDatasource"
                                      create-statements:
                                        - |
                                          CREATE TABLE documents (
                                          filename TEXT,
                                          chunk_id int,
                                          num_tokens int,
                                          lang TEXT,
                                          text TEXT,
                                          embeddings_vector FLOATA,
                                          PRIMARY KEY (filename, chunk_id));
                                topics:
                                  - name: "sink-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Write"
                                    type: "vector-db-sink"
                                    input: sink-topic
                                    id: step1
                                    configuration:
                                      datasource: "JdbcDatasource"
                                      table-name: "documents"
                                      fields:
                                        - name: "filename"
                                          expression: "value.filename"
                                          primary-key: true
                                        - name: "chunk_id"
                                          expression: "value.chunk_id"
                                          primary-key: true
                                        - name: "embeddings_vector"
                                          expression: "fn:toListOfFloat(value.embeddings_vector)"
                                        - name: "lang"
                                          expression: "value.language"
                                        - name: "text"
                                          expression: "value.text"
                                        - name: "num_tokens"
                                          expression: "value.chunk_num_tokens"
                                """);

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                        configuration:
                          resources:
                            - type: "datasource"
                              name: "JdbcDatasource"
                              configuration:
                                service: "jdbc"
                                driverClass: "herddb.jdbc.Driver"
                                url: "%s"
                                user: "sa"
                                password: "hdb"
                            - type: "open-ai-configuration"
                              name: "OpenAI Azure configuration"
                              configuration:
                                url: "%s"
                                access-key: "%s"
                                provider: "azure"
                                """
                                .formatted(
                                        jdbcUrl,
                                        wireMockRuntimeInfo.getHttpBaseUrl(),
                                        "sdòflkjsòlfkj"),
                        "module.yaml",
                        """
                               topics:
                                 - name: "input-topic"
                                   creation-mode: create-if-not-exists
                                 - name: "flare-loop-input-topic"
                                   creation-mode: create-if-not-exists
                                 - name: "output-topic"
                                   creation-mode: create-if-not-exists
                               pipeline:
                                 # Add the text of the initial task to the list of documents to retrieve
                                 # and prepare the structure
                                 - name: "init-structure"
                                   id: "kickstart-chat"
                                   type: "document-to-json"
                                   input: "input-topic"
                                   configuration:
                                     text-field: "text"
                                 - name: "kickstart-document-retrieval"
                                   type: "compute"
                                   output: "flare-loop-input-topic"
                                   configuration:
                                     fields:
                                       - name: "value.documents_to_retrieve"
                                         expression: "fn:listAdd(fn:emptyList(), value.text)"
                                       - name: "value.related_documents"
                                         expression: "fn:emptyList()"

                                 ## Flare loop
                                 # for each document to retrieve we compute the embeddings vector
                                 # documents_to_retrieve: [ { text: "the text", embeddings: [1,2,3] }, .... ]
                                 - name: "convert-docs-to-struct"
                                   id: "flare-loop"
                                   type: "compute"
                                   input: "flare-loop-input-topic"
                                   configuration:
                                     fields:
                                       - name: "value.documents_to_retrieve"
                                         expression: "fn:listToListOfStructs(value.documents_to_retrieve, 'text')"
                                       - name: "value.related_documents"
                                         expression: "fn:emptyList()"
                                 - name: "compute-embeddings"
                                   type: "compute-ai-embeddings"
                                   configuration:
                                     loop-over: "value.documents_to_retrieve"
                                     model: "text-embeddings-ada"
                                     embeddings-field: "record.embeddings"
                                     text: "{{ record.text }}"
                                     flush-interval: 0
                                 # for each document we query the vector database
                                 # the result goes into "value.retrieved_documents"
                                 - name: "lookup-related-documents"
                                   type: "query-vector-db"
                                   configuration:
                                     datasource: "JdbcDatasource"
                                     # execute the agent for all the document in documents_to_retrieve
                                     # you can refer to each document with "record.xxx"
                                     loop-over: "value.documents_to_retrieve"
                                     query: |
                                             SELECT text,embeddings_vector
                                             FROM documents
                                             ORDER BY cosine_similarity(embeddings_vector, CAST(? as FLOAT ARRAY)) DESC LIMIT 5
                                     fields:
                                       - "record.embeddings"
                                     # as we are looping over a list of document, the result of the query
                                     # is the union of all the results
                                     output-field: "value.retrieved_documents"
                                 - name: "add-documents-to-list"
                                   type: "compute"
                                   configuration:
                                       fields:
                                         # now we add all the retrieved_documents tp the list
                                         # of documents to pass to the LLM
                                         - name: "value.related_documents"
                                           expression: "fn:addAll(value.related_documents, value.retrieved_documents)"
                                         # reset previous list (not needed, but clearer)
                                         - name: "value.retrieved_documents"
                                           expression: "fn:emptyList()"
                                         - name: "value.documents_to_retrieve"
                                           expression: "fn:emptyList()"
                                 - name: "query-the-LLM"
                                   type: "ai-text-completions"
                                   configuration:
                                     model: "gp-3.5-turbo-instruct"
                                     completion-field: "value.result"
                                     logprobs: 5
                                     logprobs-field: "value.tokens"
                                     max-tokens: 100
                                     prompt:
                                         - |
                                             There is a list of documents that you must use to perform your task.
                                             Do not provide information that is not related to the provided documents.
                                            \s
                                             {{# value.related_documents}}
                                             {{text}}
                                             {{/ value.related_documents}}
                                      \s
                                             This is the task:
                                             {{ value.text }}

                                 - name: "ensure-quality-of-result"
                                   type: "flare-controller"
                                   configuration:
                                       tokens-field: "value.tokens.tokens"
                                       logprobs-field: "value.tokens.logprobs"
                                       loop-topic: "flare-loop-input-topic"
                                       retrieve-documents-field: "value.documents_to_retrieve"
                                 - name: "cleanup-response"
                                   type: "compute"
                                   output: "output-topic"
                                   configuration:
                                     fields:
                                       - name: "value"
                                         expression: "value.result"
                                """);

        // write some data
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", applicationWriter, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer(); ) {
                for (int i = 0; i < 10; i++) {
                    sendMessage(
                            "sink-topic",
                            """
                                    {
                                         "filename": "doc%s.pdf",
                                            "chunk_id": 1,
                                            "embeddings_vector": [%s,2,3,4,5],
                                            "language": "en",
                                            "text": "text%s",
                                            "chunk_num_tokens": 10
                                    }
                                    """
                                    .formatted(i, i, i),
                            producer);
                }
                executeAgentRunners(applicationRuntime);
            }
        }

        // query the database with re-rank
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml(),
                        "app-kickstart-chat",
                        "app-flare-loop")) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "this is a question", producer);
                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "I am an AI language model and I do not have personal experiences or the"));
            }
        }
    }
}
