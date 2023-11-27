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
from __future__ import annotations
import logging
import json
from functools import partial
from typing import Any, cast, Dict, Iterable, List, Optional, Tuple

import httpx

from astrapy.defaults import (
    DEFAULT_AUTH_HEADER,
    DEFAULT_JSON_API_PATH,
    DEFAULT_JSON_API_VERSION,
    DEFAULT_KEYSPACE_NAME,
)
from astrapy.utils import make_payload, make_request, http_methods
from astrapy.types import API_DOC, API_RESPONSE, PaginableRequestMethod


logger = logging.getLogger(__name__)


class AstraDBCollection:
    # Initialize the shared httpx client as a class attribute
    client = httpx.Client()

    def __init__(
        self,
        collection_name: str,
        astra_db: Optional[AstraDB] = None,
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        """
        Initialize an AstraDBCollection instance.
        Args:
            collection_name (str): The name of the collection.
            astra_db (AstraDB, optional): An instance of Astra DB.
            token (str, optional): Authentication token for Astra DB.
            api_endpoint (str, optional): API endpoint URL.
            namespace (str, optional): Namespace for the database.
        """
        # Check for presence of the Astra DB object
        if astra_db is None:
            if token is None or api_endpoint is None:
                raise AssertionError("Must provide token and api_endpoint")

            astra_db = AstraDB(
                token=token, api_endpoint=api_endpoint, namespace=namespace
            )

        # Set the remaining instance attributes
        self.astra_db = astra_db
        self.collection_name = collection_name
        self.base_path = f"{self.astra_db.base_path}/{self.collection_name}"

    def __repr__(self) -> str:
        return f'Astra DB Collection[name="{self.collection_name}", endpoint="{self.astra_db.base_url}"]'

    def _request(
        self,
        method: str = http_methods.POST,
        path: Optional[str] = None,
        json_data: Optional[Dict[str, Any]] = None,
        url_params: Optional[Dict[str, Any]] = None,
        skip_error_check: bool = False,
        **kwargs: Any,
    ) -> API_RESPONSE:
        response = make_request(
            client=self.client,
            base_url=self.astra_db.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.astra_db.token,
            method=method,
            path=path,
            json_data=json_data,
            url_params=url_params,
        )
        responsebody = cast(API_RESPONSE, response.json())

        if not skip_error_check and "errors" in responsebody:
            raise ValueError(json.dumps(responsebody["errors"]))
        else:
            return responsebody

    def _get(
        self, path: Optional[str] = None, options: Optional[Dict[str, Any]] = None
    ) -> Optional[API_RESPONSE]:
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self._request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response
        return None

    def _put(
        self, path: Optional[str] = None, document: Optional[API_RESPONSE] = None
    ) -> API_RESPONSE:
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self._request(
            method=http_methods.PUT, path=full_path, json_data=document
        )
        return response

    def _post(
        self, path: Optional[str] = None, document: Optional[API_DOC] = None
    ) -> API_RESPONSE:
        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=document
        )
        return response

    def _pre_process_find(
        self, vector: List[float], fields: Optional[List[str]] = None
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        # Must pass a vector
        if not vector:
            raise ValueError("Must pass a vector")

        # Edge case for field selection
        if fields and "$similarity" in fields:
            raise ValueError("Please use the `include_similarity` parameter")

        # Build the new vector parameter
        sort: Dict[str, Any] = {"$vector": vector}

        # Build the new fields parameter
        # Note: do not leave projection={}, make it None
        # (or it will devour $similarity away in the API response)
        if fields is not None and len(fields) > 0:
            projection = {f: 1 for f in fields}
        else:
            projection = None

        return sort, projection

    def _finalize_document_from_find(
        self, itm: API_DOC, include_similarity: bool = True
    ) -> API_DOC:
        # Clean away the returned similarity score if so desired
        if include_similarity:
            if "$similarity" not in itm:
                raise ValueError("Expected '$similarity' not found in document.")
            return itm
        else:
            return {k: v for k, v in itm.items() if k != "$similarity"}

    def get(self, path: Optional[str] = None) -> Optional[API_RESPONSE]:
        """
        Retrieve a document from the collection by its path.
        Args:
            path (str, optional): The path of the document to retrieve.
        Returns:
            dict: The retrieved document.
        """
        return self._get(path=path)

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[Dict[str, Any]] = {},
        options: Optional[Dict[str, Any]] = None,
    ) -> API_RESPONSE:
        """
        Find documents in the collection that match the given filter.
        Args:
            filter (dict, optional): Criteria to filter documents.
            projection (dict, optional): Specifies the fields to return.
            sort (dict, optional): Specifies the order in which to return matching documents.
            options (dict, optional): Additional options for the query.
        Returns:
            dict: The query response containing matched documents.
        """
        json_query = make_payload(
            top_level="find",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._post(
            document=json_query,
        )

        return response

    def vector_find(
        self,
        vector: List[float],
        *,
        limit: int,
        filter: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        include_similarity: bool = True,
    ) -> List[API_DOC]:
        """
        Perform a vector-based search in the collection.
        Args:
            vector (list): The vector to search with.
            limit (int): The maximum number of documents to return.
            filter (dict, optional): Criteria to filter documents.
            fields (list, optional): Specifies the fields to return.
            include_similarity (bool, optional): Whether to include similarity score in the result.
        Returns:
            list: A list of documents matching the vector search criteria.
        """
        # Must pass a limit
        if not limit:
            raise ValueError("Must pass a limit")

        # Pre-process the included arguments
        sort, projection = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find(
            filter=filter,
            projection=projection,
            sort=sort,
            options={
                "limit": limit,
                "includeSimilarity": include_similarity,
            },
        )

        # Post-process the return
        find_result = [
            self._finalize_document_from_find(
                raw_doc, include_similarity=include_similarity
            )
            for raw_doc in raw_find_result["data"]["documents"]
        ]

        return find_result

    @staticmethod
    def paginate(
        *, request_method: PaginableRequestMethod, options: Optional[Dict[str, Any]]
    ) -> Iterable[API_DOC]:
        """
        Generate paginated results for a given database query method.
        Args:
            request_method (function): The database query method to paginate.
            options (dict): Options for the database query.
            kwargs: Additional arguments to pass to the database query method.
        Yields:
            dict: The next document in the paginated result set.
        """
        _options = options or {}
        response0 = request_method(options=_options)
        next_page_state = response0["data"]["nextPageState"]
        options0 = _options
        for document in response0["data"]["documents"]:
            yield document
        while next_page_state is not None:
            options1 = {**options0, **{"pagingState": next_page_state}}
            response1 = request_method(options=options1)
            for document in response1["data"]["documents"]:
                yield document
            next_page_state = response1["data"]["nextPageState"]

    def paginated_find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterable[API_DOC]:
        """
        Perform a paginated search in the collection.
        Args:
            filter (dict, optional): Criteria to filter documents.
            projection (dict, optional): Specifies the fields to return.
            sort (dict, optional): Specifies the order in which to return matching documents.
            options (dict, optional): Additional options for the query.
        Returns:
            generator: A generator yielding documents in the paginated result set.
        """
        partialed_find = partial(
            self.find,
            filter=filter,
            projection=projection,
            sort=sort,
        )
        return self.paginate(
            request_method=partialed_find,
            options=options,
        )

    def pop(
        self, filter: Dict[str, Any], pop: Dict[str, Any], options: Dict[str, Any]
    ) -> API_RESPONSE:
        """
        Pop the last data in the tags array
        Args:
            filter (dict): Criteria to identify the document to update.
            pop (dict): The pop to apply to the tags.
            options (dict): Additional options for the update operation.
        Returns:
            dict: The original document before the update.
        """
        json_query = make_payload(
            top_level="findOneAndUpdate",
            filter=filter,
            update={"$pop": pop},
            options=options,
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def push(
        self, filter: Dict[str, Any], push: Dict[str, Any], options: Dict[str, Any]
    ) -> API_RESPONSE:
        """
        Push new data to the tags array
        Args:
            filter (dict): Criteria to identify the document to update.
            push (dict): The push to apply to the tags.
            options (dict): Additional options for the update operation.
        Returns:
            dict: The result of the update operation.
        """
        json_query = make_payload(
            top_level="findOneAndUpdate",
            filter=filter,
            update={"$push": push},
            options=options,
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def find_one_and_replace(
        self,
        replacement: Optional[Dict[str, Any]] = None,
        *,
        sort: Optional[Dict[str, Any]] = {},
        filter: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> API_RESPONSE:
        """
        Find a single document and replace it.
        Args:
            replacement (dict): The new document to replace the existing one.
            filter (dict, optional): Criteria to filter documents.
            sort (dict, optional): Specifies the order in which to find the document.
            options (dict, optional): Additional options for the operation.
        Returns:
            dict: The result of the find and replace operation.
        """
        json_query = make_payload(
            top_level="findOneAndReplace",
            filter=filter,
            replacement=replacement,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def vector_find_one_and_replace(
        self,
        vector: List[float],
        replacement: Dict[str, Any],
        *,
        filter: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
    ) -> API_DOC:
        """
        Perform a vector-based search and replace the first matched document.
        Args:
            vector (dict): The vector to search with.
            replacement (dict): The new document to replace the existing one.
            filter (dict, optional): Criteria to filter documents.
            fields (list, optional): Specifies the fields to return in the result.
        Returns:
            dict: The result of the vector find and replace operation.
        """
        # Pre-process the included arguments
        sort, _ = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            sort=sort,
        )

        # Post-process the return
        find_result = self._finalize_document_from_find(
            raw_find_result["data"]["document"],
            include_similarity=False,
        )

        return find_result

    def find_one_and_update(
        self,
        sort: Optional[Dict[str, Any]] = {},
        update: Optional[Dict[str, Any]] = None,
        filter: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> API_RESPONSE:
        """
        Find a single document and update it.
        Args:
            sort (dict, optional): Specifies the order in which to find the document.
            update (dict, optional): The update to apply to the document.
            filter (dict, optional): Criteria to filter documents.
            options (dict, optional): Additional options for the operation.
        Returns:
            dict: The result of the find and update operation.
        """
        json_query = make_payload(
            top_level="findOneAndUpdate",
            filter=filter,
            update=update,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

        return response

    def vector_find_one_and_update(
        self,
        vector: List[float],
        update: Dict[str, Any],
        *,
        filter: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
    ) -> API_DOC:
        """
        Perform a vector-based search and update the first matched document.
        Args:
            vector (list): The vector to search with.
            update (dict): The update to apply to the matched document.
            filter (dict, optional): Criteria to filter documents before applying the vector search.
            fields (list, optional): Specifies the fields to return in the updated document.
        Returns:
            dict: The result of the vector-based find and update operation.
        """
        # Pre-process the included arguments
        sort, _ = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find_one_and_update(
            update=update,
            filter=filter,
            sort=sort,
        )

        # Post-process the return
        find_result = self._finalize_document_from_find(
            raw_find_result["data"]["document"],
            include_similarity=False,
        )

        return find_result

    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = {},
        projection: Optional[Dict[str, Any]] = {},
        sort: Optional[Dict[str, Any]] = {},
        options: Optional[Dict[str, Any]] = {},
    ) -> API_RESPONSE:
        """
        Find a single document in the collection.
        Args:
            filter (dict, optional): Criteria to filter documents.
            projection (dict, optional): Specifies the fields to return.
            sort (dict, optional): Specifies the order in which to return the document.
            options (dict, optional): Additional options for the query.
        Returns:
            dict: The found document or None if no matching document is found.
        """
        json_query = make_payload(
            top_level="findOne",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._post(
            document=json_query,
        )

        return response

    def vector_find_one(
        self,
        vector: List[float],
        *,
        filter: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        include_similarity: bool = True,
    ) -> API_DOC:
        """
        Perform a vector-based search to find a single document in the collection.
        Args:
            vector (list): The vector to search with.
            filter (dict, optional): Additional criteria to filter documents.
            fields (list, optional): Specifies the fields to return in the result.
            include_similarity (bool, optional): Whether to include similarity score in the result.
        Returns:
            dict: The found document or None if no matching document is found.
        """
        # Pre-process the included arguments
        sort, projection = self._pre_process_find(
            vector,
            fields=fields,
        )

        # Call the underlying find() method to search
        raw_find_result = self.find_one(
            filter=filter,
            projection=projection,
            sort=sort,
            options={"includeSimilarity": include_similarity},
        )

        # Post-process the return
        find_result = self._finalize_document_from_find(
            raw_find_result["data"]["document"],
            include_similarity=include_similarity,
        )

        return find_result

    def insert_one(
        self, document: API_DOC, failures_allowed: bool = False
    ) -> API_RESPONSE:
        """
        Insert a single document into the collection.
        Args:
            document (dict): The document to insert.
            failures_allowed (bool): Whether to allow failures in the insert operation.
        Returns:
            dict: The response from the database after the insert operation.
        """
        json_query = make_payload(top_level="insertOne", document=document)

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
            skip_error_check=failures_allowed,
        )

        return response

    def insert_many(
        self,
        documents: List[API_DOC],
        options: Optional[Dict[str, Any]] = None,
        partial_failures_allowed: bool = False,
    ) -> API_RESPONSE:
        """
        Insert multiple documents into the collection.
        Args:
            documents (list): A list of documents to insert.
            options (dict, optional): Additional options for the insert operation.
            partial_failures_allowed (bool, optional): Whether to allow partial failures in the batch.
        Returns:
            dict: The response from the database after the insert operation.
        """
        json_query = make_payload(
            top_level="insertMany", documents=documents, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
            skip_error_check=partial_failures_allowed,
        )

        return response

    def update_one(
        self, filter: Dict[str, Any], update: Dict[str, Any]
    ) -> API_RESPONSE:
        """
        Update a single document in the collection.
        Args:
            filter (dict): Criteria to identify the document to update.
            update (dict): The update to apply to the document.
        Returns:
            dict: The response from the database after the update operation.
        """
        json_query = make_payload(top_level="updateOne", filter=filter, update=update)

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

        return response

    def replace(self, path: str, document: API_DOC) -> API_RESPONSE:
        """
        Replace a document in the collection.
        Args:
            path (str): The path to the document to replace.
            document (dict): The new document to replace the existing one.
        Returns:
            dict: The response from the database after the replace operation.
        """
        return self._put(path=path, document=document)

    def delete(self, id: str) -> API_RESPONSE:
        # TODO: Deprecate this method
        return self.delete_one(id)

    def delete_one(self, id: str) -> API_RESPONSE:
        """
        Delete a single document from the collection based on its ID.
        Args:
            id (str): The ID of the document to delete.
        Returns:
            dict: The response from the database after the delete operation.
        """
        json_query = {
            "deleteOne": {
                "filter": {"_id": id},
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def delete_many(self, filter: Dict[str, Any]) -> API_RESPONSE:
        """
        Delete many documents from the collection based on a filter condition
        Args:
            filter (dict): Criteria to identify the documents to delete.
        Returns:
            dict: The response from the database after the delete operation.
        """
        json_query = {
            "deleteMany": {
                "filter": filter,
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def delete_subdocument(self, id: str, subdoc: str) -> API_RESPONSE:
        """
        Delete a subdocument or field from a document in the collection.
        Args:
            id (str): The ID of the document containing the subdocument.
            subdoc (str): The key of the subdocument or field to remove.
        Returns:
            dict: The response from the database after the update operation.
        """
        json_query = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": {"$unset": {subdoc: ""}},
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def upsert(self, document: API_DOC) -> str:
        """
        Emulate an upsert operation for a single document in the collection.

        This method attempts to insert the document. If a document with the same _id exists, it updates the existing document.

        Args:
            document (dict): The document to insert or update.

        Returns:
            str: The _id of the inserted or updated document.
        """
        # Build the payload for the insert attempt
        result = self.insert_one(document, failures_allowed=True)

        # If the call failed, then we replace the existing doc
        if (
            "errors" in result
            and "errorCode" in result["errors"][0]
            and result["errors"][0]["errorCode"] == "DOCUMENT_ALREADY_EXISTS"
        ):
            # Now we attempt to update
            result = self.find_one_and_replace(
                replacement=document,
                filter={"_id": document["_id"]},
            )
            upserted_id = cast(str, result["data"]["document"]["_id"])
        else:
            upserted_id = cast(str, result["status"]["insertedIds"][0])

        return upserted_id


class AstraDB:
    # Initialize the shared httpx client as a class attribute
    client = httpx.Client()

    def __init__(
        self,
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        """
        Initialize an Astra DB instance.
        Args:
            token (str, optional): Authentication token for Astra DB.
            api_endpoint (str, optional): API endpoint URL.
            namespace (str, optional): Namespace for the database.
        """
        if token is None or api_endpoint is None:
            raise AssertionError("Must provide token and api_endpoint")

        if namespace is None:
            logger.info(
                f"ASTRA_DB_KEYSPACE is not set. Defaulting to '{DEFAULT_KEYSPACE_NAME}'"
            )
            namespace = DEFAULT_KEYSPACE_NAME

        # Store the API token
        self.token = token

        # Set the Base URL for the API calls
        self.base_url = api_endpoint.strip("/")

        # Set the API version and path from the call
        self.api_path = (api_path or DEFAULT_JSON_API_PATH).strip("/")
        self.api_version = (api_version or DEFAULT_JSON_API_VERSION).strip("/")

        # Set the namespace
        self.namespace = namespace

        # Finally, construct the full base path
        self.base_path = f"/{self.api_path}/{self.api_version}/{self.namespace}"

    def __repr__(self) -> str:
        return f'Astra DB[endpoint="{self.base_url}"]'

    def _request(
        self,
        method: str = http_methods.POST,
        path: Optional[str] = None,
        json_data: Optional[Dict[str, Any]] = None,
        url_params: Optional[Dict[str, Any]] = None,
        skip_error_check: bool = False,
    ) -> API_RESPONSE:
        response = make_request(
            client=self.client,
            base_url=self.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.token,
            method=method,
            path=path,
            json_data=json_data,
            url_params=url_params,
        )

        responsebody = cast(API_RESPONSE, response.json())

        if not skip_error_check and "errors" in responsebody:
            raise ValueError(json.dumps(responsebody["errors"]))
        else:
            return responsebody

    def collection(self, collection_name: str) -> AstraDBCollection:
        """
        Retrieve a collection from the database.
        Args:
            collection_name (str): The name of the collection to retrieve.
        Returns:
            AstraDBCollection: The collection object.
        """
        return AstraDBCollection(collection_name=collection_name, astra_db=self)

    def get_collections(self, options: Optional[Dict[str, Any]] = None) -> API_RESPONSE:
        """
        Retrieve a list of collections from the database.
        Args:
            options (dict, optional): Options to get the collection list
        Returns:
            dict: An object containing the list of collections in the database:
                {"status": {"collections": [...]}}
        """
        # Parse the options parameter
        if options is None:
            options = {}

        json_query = make_payload(
            top_level="findCollections",
            options=options,
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def create_collection(
        self,
        collection_name: str,
        *,
        options: Optional[Dict[str, Any]] = None,
        dimension: Optional[int] = None,
        metric: Optional[str] = None,
    ) -> AstraDBCollection:
        """
        Create a new collection in the database.
        Args:
            collection_name (str): The name of the collection to create.
            options (dict, optional): Options for the collection.
            dimension (int, optional): Dimension for vector search.
            metric (str, optional): Metric choice for vector search.
        Returns:
            AstraDBCollection: The created collection object.
        """
        # options from named params
        vector_options = {
            k: v
            for k, v in {
                "dimension": dimension,
                "metric": metric,
            }.items()
            if v is not None
        }

        # overlap/merge with stuff in options.vector
        dup_params = set((options or {}).get("vector", {}).keys()) & set(
            vector_options.keys()
        )

        # If any params are duplicated, we raise an error
        if dup_params:
            dups = ", ".join(sorted(dup_params))
            raise ValueError(
                f"Parameter(s) {dups} passed both to the method and in the options"
            )

        # Build our options dictionary if we have vector options
        if vector_options:
            options = options or {}
            options["vector"] = {
                **options.get("vector", {}),
                **vector_options,
            }
            if "dimension" not in options["vector"]:
                raise ValueError("Must pass dimension for vector collections")

        # Build the final json payload
        jsondata = {
            k: v
            for k, v in {"name": collection_name, "options": options}.items()
            if v is not None
        }

        # Make the request to the endpoint
        self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        )

        # Get the instance object as the return of the call
        return AstraDBCollection(astra_db=self, collection_name=collection_name)

    def delete_collection(self, collection_name: str) -> API_RESPONSE:
        """
        Delete a collection from the database.
        Args:
            collection_name (str): The name of the collection to delete.
        Returns:
            dict: The response from the database.
        """
        # Make sure we provide a collection name
        if not collection_name:
            raise ValueError("Must provide a collection name")

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": collection_name}},
        )

        return response

    def truncate_collection(self, collection_name: str) -> AstraDBCollection:
        """
        Truncate a collection in the database.
        Args:
            collection_name (str): The name of the collection to truncate.
        Returns:
            dict: The response from the database.
        """
        # Make sure we provide a collection name
        if not collection_name:
            raise ValueError("Must provide a collection name")

        # Retrieve the required collections from DB
        collections = self.get_collections(options={"explain": "true"})
        matches = [
            col
            for col in collections["status"]["collections"]
            if col["name"] == collection_name
        ]

        # If we didn't find it, raise an error
        if matches == []:
            raise ValueError(f"Collection {collection_name} not found")

        # Otherwise we found it, so get the collection
        existing_collection = matches[0]

        # We found it, so let's delete it
        self.delete_collection(collection_name)

        # End the function by returning the the new collection
        return self.create_collection(
            collection_name,
            options=existing_collection.get("options"),
        )
