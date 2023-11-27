"""
Facilities to manage the download of a secure-connect-bundle from an Astra DB
token.
"""
import requests  # type: ignore


GET_BUNDLE_URL_TEMPLATE = (
    "https://api.astra.datastax.com/v2/databases/{database_id}/secureBundleURL"
)


def get_astra_bundle_url(database_id: str, token: str) -> str:
    """
    Given a database ID and a token, provide a (temporarily-valid)
    URL for downloading the secure-connect-bundle.

    Courtesy of @phact (thank you!).
    """
    url = GET_BUNDLE_URL_TEMPLATE.format(database_id=database_id)

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.post(url, headers=headers)

    response_json = response.json()
    # Print the response
    if response_json is not None:
        if "errors" in response_json:
            # Handle the errors
            errors = response_json["errors"]
            if any(
                "jwt not valid" in (err.get("message") or "").lower() for err in errors
            ):
                raise ValueError("Invalid or insufficient token.")
            elif any(
                "malformed" in (err.get("message") or "").lower() for err in errors
            ):
                raise ValueError("Invalid or insufficient token.")
            else:
                raise ValueError(
                    "Generic error when fetching the URL to the secure-bundle."
                )
        else:
            return response_json["downloadURL"]
    else:
        raise ValueError(
            "Cannot get the secure-bundle URL. "
            "Check the provided database_id and token."
        )


def download_astra_bundle_url(database_id: str, token: str, out_file_path: str) -> None:
    """
    Obtain the secure-connect-bundle and save it to a specified file.
    """
    bundle_url = get_astra_bundle_url(database_id=database_id, token=token)
    bundle_data = requests.get(bundle_url)
    with open(out_file_path, "wb") as f:
        f.write(bundle_data.content)
    return
