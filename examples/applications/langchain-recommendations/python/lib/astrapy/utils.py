from __future__ import annotations
from typing import Any, Dict, Optional
import logging

import httpx

from astrapy import __version__
from astrapy.defaults import DEFAULT_TIMEOUT


logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG) # Apply if wishing to debug requests


class http_methods:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


package_name = __name__.split(".")[0]


def log_request_response(
    r: httpx.Response, json_data: Optional[Dict[str, Any]]
) -> None:
    """
    Log the details of an HTTP request and its response for debugging purposes.

    Args:
        r (requests.Response): The response object from the HTTP request.
        json_data (dict or None): The JSON payload sent with the request, if any.
    """
    logger.debug(f"Request URL: {r.url}")
    logger.debug(f"Request method: {r.request.method}")
    logger.debug(f"Request headers: {r.request.headers}")

    if json_data:
        logger.debug(f"Request payload: {json_data}")

    logger.debug(f"Response status code: {r.status_code}")
    logger.debug(f"Response headers: {r.headers}")
    logger.debug(f"Response content: {r.text}")


def make_request(
    client: httpx.Client,
    base_url: str,
    auth_header: str,
    token: str,
    method: str = http_methods.POST,
    path: Optional[str] = None,
    json_data: Optional[Dict[str, Any]] = None,
    url_params: Optional[Dict[str, Any]] = None,
) -> httpx.Response:
    """
    Make an HTTP request to a specified URL.

    Args:
        client (httpx): The httpx client for the request.
        base_url (str): The base URL for the request.
        auth_header (str): The authentication header key.
        token (str): The token used for authentication.
        method (str, optional): The HTTP method to use for the request. Default is POST.
        path (str, optional): The specific path to append to the base URL.
        json_data (dict, optional): JSON payload to be sent with the request.
        url_params (dict, optional): URL parameters to be sent with the request.

    Returns:
        requests.Response: The response from the HTTP request.
    """
    r = client.request(
        method=method,
        url=f"{base_url}{path}",
        params=url_params,
        json=json_data,
        timeout=DEFAULT_TIMEOUT,
        headers={auth_header: token, "User-Agent": f"{package_name}/{__version__}"},
    )

    if logger.isEnabledFor(logging.DEBUG):
        log_request_response(r, json_data)

    return r


def make_payload(top_level: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Construct a JSON payload for an HTTP request with a specified top-level key.

    Args:
        top_level (str): The top-level key for the JSON payload.
        **kwargs: Arbitrary keyword arguments representing other keys and their values to be included in the payload.

    Returns:
        dict: The constructed JSON payload.
    """
    params = {}
    for key, value in kwargs.items():
        params[key] = value

    json_query: Dict[str, Any] = {top_level: {}}

    # Adding keys only if they're provided
    for key, value in params.items():
        if value is not None:
            json_query[top_level][key] = value

    return json_query
