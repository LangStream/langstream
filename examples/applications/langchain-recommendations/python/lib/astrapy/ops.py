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

import logging
from typing import Any, cast, Dict, Optional

import httpx

from astrapy.utils import make_request, http_methods
from astrapy.defaults import DEFAULT_DEV_OPS_API_VERSION, DEFAULT_DEV_OPS_URL
from astrapy.types import API_RESPONSE


logger = logging.getLogger(__name__)


class AstraDBOps:
    # Initialize the shared httpx client as a class attribute
    client = httpx.Client()

    def __init__(
        self,
        token: str,
        dev_ops_url: Optional[str] = None,
        dev_ops_api_version: Optional[str] = None,
    ) -> None:
        dev_ops_url = (dev_ops_url or DEFAULT_DEV_OPS_URL).strip("/")
        dev_ops_api_version = (
            dev_ops_api_version or DEFAULT_DEV_OPS_API_VERSION
        ).strip("/")

        self.token = "Bearer " + token
        self.base_url = f"https://{dev_ops_url}/{dev_ops_api_version}"

    def _ops_request(
        self,
        method: str,
        path: str,
        options: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> httpx.Response:
        _options = {} if options is None else options

        return make_request(
            client=self.client,
            base_url=self.base_url,
            method=method,
            auth_header="Authorization",
            token=self.token,
            json_data=json_data,
            url_params=_options,
            path=path,
        )

    def _json_ops_request(
        self,
        method: str,
        path: str,
        options: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> API_RESPONSE:
        req_result = self._ops_request(
            method=method,
            path=path,
            options=options,
            json_data=json_data,
        )
        return cast(
            API_RESPONSE,
            req_result.json(),
        )

    def get_databases(self, options: Optional[Dict[str, Any]] = None) -> API_RESPONSE:
        """
        Retrieve a list of databases.

        Args:
            options (dict, optional): Additional options for the request.

        Returns:
            dict: A JSON response containing the list of databases.
        """
        response = self._json_ops_request(
            method=http_methods.GET, path="/databases", options=options
        )

        return response

    def create_database(
        self, database_definition: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, str]]:
        """
        Create a new database.

        Args:
            database_definition (dict, optional): A dictionary defining the properties of the database to be created.

        Returns:
            dict: A dictionary containing the ID of the created database, or None if creation was unsuccessful.
        """
        r = self._ops_request(
            method=http_methods.POST, path="/databases", json_data=database_definition
        )

        if r.status_code == 201:
            return {"id": r.headers["Location"]}

        return None

    def terminate_database(self, database: str = "") -> Optional[str]:
        """
        Terminate an existing database.

        Args:
            database (str): The identifier of the database to terminate.

        Returns:
            str: The identifier of the terminated database, or None if termination was unsuccessful.
        """
        r = self._ops_request(
            method=http_methods.POST, path=f"/databases/{database}/terminate"
        )

        if r.status_code == 202:
            return database

        return None

    def get_database(
        self, database: str = "", options: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Retrieve details of a specific database.

        Args:
            database (str): The identifier of the database to retrieve.
            options (dict, optional): Additional options for the request.

        Returns:
            dict: A JSON response containing the details of the specified database.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}",
            options=options,
        )

    def create_keyspace(self, database: str = "", keyspace: str = "") -> httpx.Response:
        """
        Create a keyspace in a specified database.

        Args:
            database (str): The identifier of the database where the keyspace will be created.
            keyspace (str): The name of the keyspace to create.

        Returns:
            requests.Response: The response object from the HTTP request.
        """
        return self._ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/keyspaces/{keyspace}",
        )

    def park_database(self, database: str = "") -> API_RESPONSE:
        """
        Park a specific database, making it inactive.

        Args:
            database (str): The identifier of the database to park.

        Returns:
            dict: The response from the server after parking the database.
        """
        return self._json_ops_request(
            method=http_methods.POST, path=f"/databases/{database}/park"
        )

    def unpark_database(self, database: str = "") -> API_RESPONSE:
        """
        Unpark a specific database, making it active again.

        Args:
            database (str): The identifier of the database to unpark.

        Returns:
            dict: The response from the server after unparking the database.
        """
        return self._json_ops_request(
            method=http_methods.POST, path=f"/databases/{database}/unpark"
        )

    def resize_database(
        self, database: str = "", options: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Resize a specific database according to provided options.

        Args:
            database (str): The identifier of the database to resize.
            options (dict, optional): The specifications for the resize operation.

        Returns:
            dict: The response from the server after the resize operation.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/resize",
            json_data=options,
        )

    def reset_database_password(
        self, database: str = "", options: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Reset the password for a specific database.

        Args:
            database (str): The identifier of the database for which to reset the password.
            options (dict, optional): Additional options for the password reset.

        Returns:
            dict: The response from the server after resetting the password.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/resetPassword",
            json_data=options,
        )

    def get_secure_bundle(self, database: str = "") -> API_RESPONSE:
        """
        Retrieve a secure bundle URL for a specific database.

        Args:
            database (str): The identifier of the database for which to get the secure bundle.

        Returns:
            dict: The secure bundle URL and related information.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/secureBundleURL",
        )

    def get_datacenters(self, database: str = "") -> API_RESPONSE:
        """
        Get a list of datacenters associated with a specific database.

        Args:
            database (str): The identifier of the database for which to list datacenters.

        Returns:
            dict: A list of datacenters and their details.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}/datacenters",
        )

    def create_datacenter(
        self, database: str = "", options: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Create a new datacenter for a specific database.

        Args:
            database (str): The identifier of the database for which to create the datacenter.
            options (dict, optional): Specifications for the new datacenter.

        Returns:
            dict: The response from the server after creating the datacenter.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/datacenters",
            json_data=options,
        )

    def terminate_datacenter(
        self, database: str = "", datacenter: str = ""
    ) -> API_RESPONSE:
        """
        Terminate a specific datacenter in a database.

        Args:
            database (str): The identifier of the database containing the datacenter.
            datacenter (str): The identifier of the datacenter to terminate.

        Returns:
            dict: The response from the server after terminating the datacenter.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/datacenters/{datacenter}/terminate",
        )

    def get_access_list(self, database: str = "") -> API_RESPONSE:
        """
        Retrieve the access list for a specific database.

        Args:
            database (str): The identifier of the database for which to get the access list.

        Returns:
            dict: The current access list for the database.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/databases/{database}/access-list",
        )

    def replace_access_list(
        self, database: str = "", access_list: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Replace the entire access list for a specific database.

        Args:
            database (str): The identifier of the database for which to replace the access list.
            access_list (dict): The new access list to be set.

        Returns:
            dict: The response from the server after replacing the access list.
        """
        return self._json_ops_request(
            method=http_methods.PUT,
            path=f"/databases/{database}/access-list",
            json_data=access_list,
        )

    def update_access_list(
        self, database: str = "", access_list: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Update the access list for a specific database.

        Args:
            database (str): The identifier of the database for which to update the access list.
            access_list (dict): The updates to be applied to the access list.

        Returns:
            dict: The response from the server after updating the access list.
        """
        return self._json_ops_request(
            method=http_methods.PATCH,
            path=f"/databases/{database}/access-list",
            json_data=access_list,
        )

    def add_access_list_address(
        self, database: str = "", address: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Add a new address to the access list for a specific database.

        Args:
            database (str): The identifier of the database for which to add the address.
            address (dict): The address details to add to the access list.

        Returns:
            dict: The response from the server after adding the address.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/databases/{database}/access-list",
            json_data=address,
        )

    def delete_access_list(self, database: str = "") -> API_RESPONSE:
        """
        Delete the access list for a specific database.

        Args:
            database (str): The identifier of the database for which to delete the access list.

        Returns:
            dict: The response from the server after deleting the access list.
        """
        return self._json_ops_request(
            method=http_methods.DELETE,
            path=f"/databases/{database}/access-list",
        )

    def get_private_link(self, database: str = "") -> API_RESPONSE:
        """
        Retrieve the private link information for a specified database.

        Args:
            database (str): The identifier of the database.

        Returns:
            dict: The private link information for the database.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/private-link",
        )

    def get_datacenter_private_link(
        self, database: str = "", datacenter: str = ""
    ) -> API_RESPONSE:
        """
        Retrieve the private link information for a specific datacenter in a database.

        Args:
            database (str): The identifier of the database.
            datacenter (str): The identifier of the datacenter.

        Returns:
            dict: The private link information for the specified datacenter.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/private-link",
        )

    def create_datacenter_private_link(
        self,
        database: str = "",
        datacenter: str = "",
        private_link: Optional[Dict[str, Any]] = None,
    ) -> API_RESPONSE:
        """
        Create a private link for a specific datacenter in a database.

        Args:
            database (str): The identifier of the database.
            datacenter (str): The identifier of the datacenter.
            private_link (dict): The private link configuration details.

        Returns:
            dict: The response from the server after creating the private link.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/private-link",
            json_data=private_link,
        )

    def create_datacenter_endpoint(
        self,
        database: str = "",
        datacenter: str = "",
        endpoint: Optional[Dict[str, Any]] = None,
    ) -> API_RESPONSE:
        """
        Create an endpoint for a specific datacenter in a database.

        Args:
            database (str): The identifier of the database.
            datacenter (str): The identifier of the datacenter.
            endpoint (dict): The endpoint configuration details.

        Returns:
            dict: The response from the server after creating the endpoint.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoint",
            json_data=endpoint,
        )

    def update_datacenter_endpoint(
        self, database: str = "", datacenter: str = "", endpoint: Dict[str, Any] = {}
    ) -> API_RESPONSE:
        """
        Update an existing endpoint for a specific datacenter in a database.

        Args:
            database (str): The identifier of the database.
            datacenter (str): The identifier of the datacenter.
            endpoint (dict): The updated endpoint configuration details.

        Returns:
            dict: The response from the server after updating the endpoint.
        """
        return self._json_ops_request(
            method=http_methods.PUT,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint['id']}",
            json_data=endpoint,
        )

    def get_datacenter_endpoint(
        self, database: str = "", datacenter: str = "", endpoint: str = ""
    ) -> API_RESPONSE:
        """
        Retrieve information about a specific endpoint in a datacenter of a database.

        Args:
            database (str): The identifier of the database.
            datacenter (str): The identifier of the datacenter.
            endpoint (str): The identifier of the endpoint.

        Returns:
            dict: The endpoint information for the specified datacenter.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint}",
        )

    def delete_datacenter_endpoint(
        self, database: str = "", datacenter: str = "", endpoint: str = ""
    ) -> API_RESPONSE:
        """
        Delete a specific endpoint in a datacenter of a database.

        Args:
            database (str): The identifier of the database.
            datacenter (str): The identifier of the datacenter.
            endpoint (str): The identifier of the endpoint to delete.

        Returns:
            dict: The response from the server after deleting the endpoint.
        """
        return self._json_ops_request(
            method=http_methods.DELETE,
            path=f"/organizations/clusters/{database}/datacenters/{datacenter}/endpoints/{endpoint}",
        )

    def get_available_classic_regions(self) -> API_RESPONSE:
        """
        Retrieve a list of available classic regions.

        Returns:
            dict: A list of available classic regions.
        """
        return self._json_ops_request(method=http_methods.GET, path="/availableRegions")

    def get_available_regions(self) -> API_RESPONSE:
        """
        Retrieve a list of available regions for serverless deployment.

        Returns:
            dict: A list of available regions for serverless deployment.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/regions/serverless"
        )

    def get_roles(self) -> API_RESPONSE:
        """
        Retrieve a list of roles within the organization.

        Returns:
            dict: A list of roles within the organization.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/organizations/roles"
        )

    def create_role(
        self, role_definition: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Create a new role within the organization.

        Args:
            role_definition (dict, optional): The definition of the role to be created.

        Returns:
            dict: The response from the server after creating the role.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path="/organizations/roles",
            json_data=role_definition,
        )

    def get_role(self, role: str = "") -> API_RESPONSE:
        """
        Retrieve details of a specific role within the organization.

        Args:
            role (str): The identifier of the role.

        Returns:
            dict: The details of the specified role.
        """
        return self._json_ops_request(
            method=http_methods.GET, path=f"/organizations/roles/{role}"
        )

    def update_role(
        self, role: str = "", role_definition: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Update the definition of an existing role within the organization.

        Args:
            role (str): The identifier of the role to update.
            role_definition (dict, optional): The new definition of the role.

        Returns:
            dict: The response from the server after updating the role.
        """
        return self._json_ops_request(
            method=http_methods.PUT,
            path=f"/organizations/roles/{role}",
            json_data=role_definition,
        )

    def delete_role(self, role: str = "") -> API_RESPONSE:
        """
        Delete a specific role from the organization.

        Args:
            role (str): The identifier of the role to delete.

        Returns:
            dict: The response from the server after deleting the role.
        """
        return self._json_ops_request(
            method=http_methods.DELETE, path=f"/organizations/roles/{role}"
        )

    def invite_user(
        self, user_definition: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Invite a new user to the organization.

        Args:
            user_definition (dict, optional): The definition of the user to be invited.

        Returns:
            dict: The response from the server after inviting the user.
        """
        return self._json_ops_request(
            method=http_methods.PUT,
            path="/organizations/users",
            json_data=user_definition,
        )

    def get_users(self) -> API_RESPONSE:
        """
        Retrieve a list of users within the organization.

        Returns:
            dict: A list of users within the organization.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/organizations/users"
        )

    def get_user(self, user: str = "") -> API_RESPONSE:
        """
        Retrieve details of a specific user within the organization.

        Args:
            user (str): The identifier of the user.

        Returns:
            dict: The details of the specified user.
        """
        return self._json_ops_request(
            method=http_methods.GET, path=f"/organizations/users/{user}"
        )

    def remove_user(self, user: str = "") -> API_RESPONSE:
        """
        Remove a user from the organization.

        Args:
            user (str): The identifier of the user to remove.

        Returns:
            dict: The response from the server after removing the user.
        """
        return self._json_ops_request(
            method=http_methods.DELETE, path=f"/organizations/users/{user}"
        )

    def update_user_roles(
        self, user: str = "", roles: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Update the roles assigned to a specific user within the organization.

        Args:
            user (str): The identifier of the user.
            roles (list, optional): The list of new roles to assign to the user.

        Returns:
            dict: The response from the server after updating the user's roles.
        """
        return self._json_ops_request(
            method=http_methods.PUT,
            path=f"/organizations/users/{user}/roles",
            json_data=roles,
        )

    def get_clients(self) -> API_RESPONSE:
        """
        Retrieve a list of client IDs and secrets associated with the organization.

        Returns:
            dict: A list of client IDs and their associated secrets.
        """
        return self._json_ops_request(method=http_methods.GET, path="/clientIdSecrets")

    def create_token(self, roles: Optional[Dict[str, Any]] = None) -> API_RESPONSE:
        """
        Create a new token with specific roles.

        Args:
            roles (dict, optional): The roles to associate with the token:
                {"roles": ["<roleId>"]}

        Returns:
            dict: The response from the server after creating the token.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path="/clientIdSecrets",
            json_data=roles,
        )

    def delete_token(self, token: str = "") -> API_RESPONSE:
        """
        Delete a specific token.

        Args:
            token (str): The identifier of the token to delete.

        Returns:
            dict: The response from the server after deleting the token.
        """
        return self._json_ops_request(
            method=http_methods.DELETE, path=f"/clientIdSecret/{token}"
        )

    def get_organization(self) -> API_RESPONSE:
        """
        Retrieve details of the current organization.

        Returns:
            dict: The details of the organization.
        """
        return self._json_ops_request(method=http_methods.GET, path="/currentOrg")

    def get_access_lists(self) -> API_RESPONSE:
        """
        Retrieve a list of access lists for the organization.

        Returns:
            dict: A list of access lists.
        """
        return self._json_ops_request(method=http_methods.GET, path="/access-lists")

    def get_access_list_template(self) -> API_RESPONSE:
        """
        Retrieve a template for creating an access list.

        Returns:
            dict: An access list template.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/access-list/template"
        )

    def validate_access_list(self) -> API_RESPONSE:
        """
        Validate the configuration of the access list.

        Returns:
            dict: The validation result of the access list configuration.
        """
        return self._json_ops_request(
            method=http_methods.POST, path="/access-list/validate"
        )

    def get_private_links(self) -> API_RESPONSE:
        """
        Retrieve a list of private link connections for the organization.

        Returns:
            dict: A list of private link connections.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/organizations/private-link"
        )

    def get_streaming_providers(self) -> API_RESPONSE:
        """
        Retrieve a list of streaming service providers.

        Returns:
            dict: A list of available streaming service providers.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/streaming/providers"
        )

    def get_streaming_tenants(self) -> API_RESPONSE:
        """
        Retrieve a list of streaming tenants.

        Returns:
            dict: A list of streaming tenants and their details.
        """
        return self._json_ops_request(
            method=http_methods.GET, path="/streaming/tenants"
        )

    def create_streaming_tenant(
        self, tenant: Optional[Dict[str, Any]] = None
    ) -> API_RESPONSE:
        """
        Create a new streaming tenant.

        Args:
            tenant (dict, optional): The configuration details for the new streaming tenant.

        Returns:
            dict: The response from the server after creating the streaming tenant.
        """
        return self._json_ops_request(
            method=http_methods.POST,
            path="/streaming/tenants",
            json_data=tenant,
        )

    def delete_streaming_tenant(self, tenant: str = "", cluster: str = "") -> None:
        """
        Delete a specific streaming tenant from a cluster.

        Args:
            tenant (str): The identifier of the tenant to delete.
            cluster (str): The identifier of the cluster from which the tenant is to be deleted.

        Returns:
            dict: The response from the server after deleting the streaming tenant.
        """
        r = self._ops_request(
            method=http_methods.DELETE,
            path=f"/streaming/tenants/{tenant}/clusters/{cluster}",
        )

        if r.status_code == 202:  # 'Accepted'
            return None
        else:
            raise ValueError(r.text)

    def get_streaming_tenant(self, tenant: str = "") -> API_RESPONSE:
        """
        Retrieve information about the limits and usage of a specific streaming tenant.

        Args:
            tenant (str): The identifier of the streaming tenant.

        Returns:
            dict: Details of the specified streaming tenant, including limits and current usage.
        """
        return self._json_ops_request(
            method=http_methods.GET,
            path=f"/streaming/tenants/{tenant}/limits",
        )
