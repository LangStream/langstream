#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from langstream import Processor, SimpleRecord
from operator import itemgetter
from typing import Dict, List, Optional, Sequence, Any
from langchain.vectorstores.cassandra import Cassandra
from langchain.callbacks.manager import Callbacks, collect_runs
from langchain.chat_models import ChatOpenAI
from langchain.embeddings import OpenAIEmbeddings
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder, PromptTemplate
from langchain.schema import Document
from langchain.schema.language_model import BaseLanguageModel
from langchain.schema.messages import AIMessage, HumanMessage
from langchain.schema.output_parser import StrOutputParser
from langchain.schema.retriever import BaseRetriever
from langchain.schema.runnable import (
    Runnable,
    RunnableBranch,
    RunnableLambda,
    RunnableMap,
)
from pydantic import BaseModel
import cassio
import logging

RESPONSE_TEMPLATE = """\
You are an expert programmer and problem-solver, tasked with answering any question \
about Langchain.

Generate a comprehensive and informative answer of 80 words or less for the \
given question based solely on the provided search results (URL and content). You must \
only use information from the provided search results. Use an unbiased and \
journalistic tone. Combine search results together into a coherent answer. Do not \
repeat text. Cite search results using [${{number}}] notation. Only cite the most \
relevant results that answer the question accurately. Place these citations at the end \
of the sentence or paragraph that reference them - do not put them all at the end. If \
different results refer to different entities within the same name, write separate \
answers for each entity.

You should use bullet points in your answer for readability. Put citations where they apply
rather than putting them all at the end.

If there is nothing in the context relevant to the question at hand, just say "Hmm, \
I'm not sure." Don't try to make up an answer.

Anything between the following `context`  html blocks is retrieved from a knowledge \
bank, not part of the conversation with the user. 

<context>
    {context} 
<context/>

REMEMBER: If there is no relevant information within the context, just say "Hmm, I'm \
not sure." Don't try to make up an answer. Anything between the preceding 'context' \
html blocks is retrieved from a knowledge bank, not part of the conversation with the \
user.\
"""

REPHRASE_TEMPLATE = """\
Given the following conversation and a follow up question, rephrase the follow up \
question to be a standalone question.

Chat History:
{chat_history}
Follow Up Input: {question}
Standalone Question:"""


class ChatRequest(BaseModel):
    question: str
    chat_history: Optional[List[Dict[str, str]]]


class FakeRetriever(BaseRetriever):
    def _get_relevant_documents(
            self,
            query: str,
            *,
            callbacks: Callbacks = None,
            tags: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None,
            **kwargs: Any,
    ) -> List[Document]:
        return [Document(page_content="foo"), Document(page_content="bar")]

    async def _aget_relevant_documents(
            self,
            query: str,
            *,
            callbacks: Callbacks = None,
            tags: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None,
            **kwargs: Any,
    ) -> List[Document]:
        return [Document(page_content="foo"), Document(page_content="bar")]


def get_retriever(embeddingsModel) -> BaseRetriever:
    return FakeRetriever()


def create_retriever_chain(
        llm: BaseLanguageModel, retriever: BaseRetriever
) -> Runnable:
    CONDENSE_QUESTION_PROMPT = PromptTemplate.from_template(REPHRASE_TEMPLATE)
    condense_question_chain = (
            CONDENSE_QUESTION_PROMPT | llm | StrOutputParser()
    ).with_config(
        run_name="CondenseQuestion",
    )
    conversation_chain = condense_question_chain | retriever
    return RunnableBranch(
        (
            RunnableLambda(lambda x: bool(x.get("chat_history"))).with_config(
                run_name="HasChatHistoryCheck"
            ),
            conversation_chain.with_config(run_name="RetrievalChainWithHistory"),
        ),
        (
                RunnableLambda(itemgetter("question")).with_config(
                    run_name="Itemgetter:question"
                )
                | retriever
        ).with_config(run_name="RetrievalChainWithNoHistory"),
    ).with_config(run_name="RouteDependingOnChatHistory")


def format_docs(docs: Sequence[Document]) -> str:
    formatted_docs = []
    for i, doc in enumerate(docs):
        doc_string = f"<doc id='{i}'>{doc.page_content}</doc>"
        formatted_docs.append(doc_string)
    return "\n".join(formatted_docs)


def serialize_history(request: ChatRequest):
    chat_history = request["chat_history"] or []
    converted_chat_history = []
    for message in chat_history:
        if message.get("human") is not None:
            converted_chat_history.append(HumanMessage(content=message["human"]))
        if message.get("ai") is not None:
            converted_chat_history.append(AIMessage(content=message["ai"]))
    return converted_chat_history


def create_chain(
        llm: BaseLanguageModel,
        retriever: BaseRetriever,
) -> Runnable:
    retriever_chain = create_retriever_chain(
        llm,
        retriever,
    ).with_config(run_name="FindDocs")
    _context = RunnableMap(
        {
            "context": retriever_chain | format_docs,
            "question": itemgetter("question"),
            "chat_history": itemgetter("chat_history"),
        }
    ).with_config(run_name="RetrieveDocs")
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", RESPONSE_TEMPLATE),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}"),
        ]
    )

    response_synthesizer = (prompt | llm | StrOutputParser()).with_config(
        run_name="GenerateResponse",
    )
    return (
            {
                "question": RunnableLambda(itemgetter("question")).with_config(
                    run_name="Itemgetter:question"
                ),
                "chat_history": RunnableLambda(serialize_history).with_config(
                    run_name="SerializeHistory"
                ),
            }
            | _context
            | response_synthesizer
    )


class LangChainChat(Processor):

    def init(self, config):
        logging.info("Initializing LangChain Chat with config %s", config)
        # the values are configured in the resources section in configuration.yaml
        self.openai_key = config["resources"]["openai"]["access-key"]
        self.astra_db_token = config["resources"]["database"]["token"]
        self.astra_db_id = config["resources"]["database"]["database-id"]
        self.astra_keyspace = config["astra-db-keyspace"]
        self.astra_table_name = config["astra-db-table"]

        cassio.init(token=self.astra_db_token, database_id=self.astra_db_id)

        self.embedings_model = OpenAIEmbeddings(openai_api_key=self.openai_key)

        self.astra_vector_store = Cassandra(
            embedding=self.embedings_model,
            session=None,
            keyspace=self.astra_keyspace,
            table_name=self.astra_table_name,
        )

        self.llm = ChatOpenAI(
            openai_api_key=self.openai_key,
            model="gpt-3.5-turbo-16k",
            streaming=True,
            temperature=0,
        )

        self.retriever = self.astra_vector_store.as_retriever()

        self.answer_chain = create_chain(
            self.llm,
            self.retriever,
        )

    def process(self, record):
        question = record.value()
        response = self.answer_chain.invoke({
            "question": question,
            "chat_history": []
        })
        return [SimpleRecord(response, headers=record.headers())]
