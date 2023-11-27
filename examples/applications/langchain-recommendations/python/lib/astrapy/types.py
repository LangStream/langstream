from __future__ import annotations
from typing import Any, Dict, Protocol

# A type for:
#     "dict from parsing a JSON from the API responses"
# This is not exactly == the JSON specs,
# (e.g. 'null' is valid JSON), but the JSON API are committed to always
# return JSON objects with a mapping as top-level.
API_RESPONSE = Dict[str, Any]

# A type for:
#     "document stored on the collections"
# Identical to the above in its nature, but preferrably marked as
# 'a disting thing, conceptually, from the returned JSONs'
API_DOC = Dict[str, Any]


# This is for the (partialed, if necessary) functions that can be "paginated".
class PaginableRequestMethod(Protocol):
    def __call__(self, options: Dict[str, Any]) -> API_RESPONSE:
        ...
