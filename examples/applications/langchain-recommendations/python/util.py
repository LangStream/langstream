import os

from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores.astradb import AstraDB
from astrapy.db import (
    AstraDB as LibAstraDB,
)

PRODUCTS_COLLECTION_NAME = "products"
USERS_COLLECTION_NAME = "users"
RECOMMENDATIONS_COLLECTION_NAME = "recommendations"


def get_openai_token() -> str:
    return os.getenv("OPENAI_API_KEY",
                     "")


def create_astra_vector_store(collection: str) -> AstraDB:
    return AstraDB(
        collection_name=collection,
        embedding=OpenAIEmbeddings(openai_api_key=get_openai_token()),
        token=get_astra_token(),
        api_endpoint=get_astra_api_endpoint()
    )


def create_raw_astra_client() -> LibAstraDB:
    return LibAstraDB(
        token=get_astra_token(),
        api_endpoint=get_astra_api_endpoint()
    )


def get_astra_api_endpoint():
    return os.getenv("DEMO_PRODUCTS_ASTRA_DB_ENDPOINT", "")


def get_astra_token():
    return os.getenv("DEMO_PRODUCTS_ASTRA_TOKEN", "")


