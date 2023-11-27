import json
import os
from operator import itemgetter
from typing import Sequence

from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.schema import StrOutputParser, Document
from langchain.schema.runnable import RunnableMap, RunnableLambda

from util import create_astra_vector_store, get_openai_token, USERS_COLLECTION_NAME, PRODUCTS_COLLECTION_NAME

RESPONSE_TEMPLATE = """
You are a helpful assistant.
The user is looking for buying a complimentary product of: "{product}". 
Create a recommendation of 1 to 4 products to buy specifically for the user.
If you don't think the user should buy anything, just not list any product.  
Each `item` in the following `products` html blocks, is a possible product to recommend. Do not make up products not listed here. \

<products>
  {possible_products}
<products/>


For each product recommended, add a field explaining why you think they would buy those product
Also include a short comparison between it and the similar product mentioned earlier. 
Keep it in less than 50 words.
Export the results in a JSON List format. 
The top level object is a list. 
Each item has a "product_info" field - as json object - containing the 'metadata' field content provided in the `<product>` html tag - so please parse it from JSON -
and a "reason" field containing the reason.
"""

def main():
    while True:
        product = input("Product?\n")
        response = get_recommendation_for_product({"name": product, "description": product})
        print("\nSuggested products:\n")
        for suggested in response:
            print(suggested["product_info"]["NAME"] + " - " + suggested["product_info"]["PRICE"] + "$. " + suggested[
                "reason"])
        print("\n")

astra_vector_store = create_astra_vector_store(PRODUCTS_COLLECTION_NAME)
retriever = astra_vector_store.as_retriever()
llm = ChatOpenAI(
    openai_api_key=get_openai_token(),
    model="gpt-4",
    temperature=0,
)
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", RESPONSE_TEMPLATE)
    ]
)

def get_recommendation_for_product(product_info):


    def format_possible_products(docs: Sequence[Document]) -> str:
        formatted_docs = []
        for doc in enumerate(docs):
            actual_doc = doc[1]
            print(actual_doc)
            metadata = json.dumps(actual_doc.metadata)

            doc_string = f"<product metadata='{metadata}'>{actual_doc.page_content}</product>"
            formatted_docs.append(doc_string)
        return "\n".join(formatted_docs)

    def format_response(text):
        as_list = json.loads(text)
        result = []
        for item in as_list:
            result.append({
                "name": item["product_info"]["NAME"],
                "description": item["product_info"]["DESCRIPTION"],
                "id": item["product_info"]["ID"],
                "price": item["product_info"]["PRICE"],
                "reason": item["reason"]
            })
        return result


    chain = (prompt | llm | StrOutputParser() | RunnableLambda(format_response)).with_config(
        run_name="GenerateResponse",
    )

    retrieve_chain = RunnableMap(
        {
            "product": itemgetter("product"),
            "possible_products": itemgetter("product_description") | retriever | format_possible_products
        }
    ).with_config(run_name="RetrieveDocs")

    response = (
            retrieve_chain | chain
    ).invoke({
        "product": product_info["name"],
        "product_description": product_info["description"]
    })
    return response


if __name__ == '__main__':
    main()
