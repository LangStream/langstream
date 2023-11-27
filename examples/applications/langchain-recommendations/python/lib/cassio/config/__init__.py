import tempfile
import shutil
import os
from typing import Any, Dict, List, Optional, Union

from cassandra.cluster import Cluster, Session  # type: ignore
from cassandra.auth import PlainTextAuthProvider  # type: ignore

from cassio.config.bundle_management import (
    init_string_to_bundle_path_and_options,
    infer_keyspace_from_bundle,
)
from cassio.config.bundle_download import download_astra_bundle_url


ASTRA_CLOUD_AUTH_USERNAME = "token"
DOWNLOADED_BUNDLE_FILE_NAME = "secure-connect-bundle_devopsapi.zip"

default_session: Optional[Session] = None
default_keyspace: Optional[str] = None


def init(
    auto: bool = False,
    session: Optional[Session] = None,
    secure_connect_bundle: Optional[str] = None,
    init_string: Optional[str] = None,
    token: Optional[str] = None,
    database_id: Optional[str] = None,
    keyspace: Optional[str] = None,
    contact_points: Optional[Union[str, List[str]]] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    cluster_kwargs: Optional[Dict[str, Any]] = None,
    tempfile_basedir: Optional[str] = None,
) -> None:
    """
    Globally set the default Cassandra connection (/keyspace) for CassIO.
    This default will be used by all other db-requiring CassIO instantiations,
    unless passed to the respective classes' __init__.

    There are various ways to achieve this, depending on the passed parameters.
    Broadly speaking, this method allows to pass one's own ready Session,
    or to have it created in the method. For this second case, both Astra DB
    and a regular Cassandra cluster can be targeted.
    One can also simply call cassio.init(auto=True) and let it figure out
    what to do based on standard environment variables.

    CASSANDRA
    If one passes `contact_points`, it is assumed that this is Cassandra.
    In that case, only the following arguments will be used:
    `contact_points`, `keyspace`, `username`, `password`, `cluster_kwargs`
    Note that when passing a `session` all other parameters are ignored.

    ASTRA DB:
    If `contact_points` is not passed, one of several methods to connect to
    Astra should be supplied for the connection to Astra. These overlap:
    see below for their precedence.
    Note that when passing a `session` all other parameters are ignored.

    AUTO:
    If passing auto=True, no other parameter is allowed except for
    `tempfile_basedir`. The rest, including choosing Astra DB / Cassandra,
    is autofilled by inspecting the following environment variables:
        CASSANDRA_CONTACT_POINTS
        CASSANDRA_USERNAME
        CASSANDRA_PASSWORD
        CASSANDRA_KEYSPACE
        ASTRA_DB_APPLICATION_TOKEN
        ASTRA_DB_INIT_STRING
        ASTRA_DB_SECURE_BUNDLE_PATH
        ASTRA_DB_KEYSPACE
        ASTRA_DB_DATABASE_ID

    PARAMETERS:
        `auto`: (bool = False), whether to auto-guess all connection params.
        `session` (optional cassandra.cluster.Session), an established connection.
        `secure_connect_bundle` (optional str), full path to a Secure Bundle.
        `init_string` (optional str), a stand-alone "db init string" credential
            (which can optionally contain keyspace and/or token).
        `token` (optional str), the Astra DB "AstraCS:..." token.
        `database_id` (optional str), the Astra DB ID. Used only for Astra
            connections with no `secure_connect_bundle` parameter passed.
        `keyspace` (optional str), the keyspace to work.
        `contact_points` (optional List[str]), for Cassandra connection
        `username` (optional str), username for Cassandra connection
        `password` (optional str), password for Cassandra connection
        `cluster_kwargs` (optional dict), additional arguments to `Cluster(...)`.
        `tempfile_basedir` (optional str), where to create temporary work directories.

    ASTRA DB:
    The Astra-related parameters are arranged in a chain of fallbacks.
    In case redundant information is supplied, these are the precedences:
        session > secure_connect_bundle > init_string
        token > (from init_string if any)
        keyspace > (from init_string if any) > (from bundle if any)
    If a secure-connect-bundle is needed and not passed, it will be downloaded:
        this requires `database_id` to be passed, suitable token permissions.
    Constraints and caveats:
        `secure_connect_bundle` requires `token`.
        `session` does not make use of `cluster_kwargs` and will ignore it.

    The Session is created at `init` time and kept around, available to any
    subsequent table creation. If calling `init` a second time, a new Session
    will be made available replacing the previous one.
    """
    global default_session
    global default_keyspace
    temp_dir_created: bool = False
    temp_dir: Optional[str] = None
    direct_session: Optional[Session] = None
    bundle_from_is: Optional[str] = None
    bundle_from_arg: Optional[str] = None
    bundle_from_download: Optional[str] = None
    keyspace_from_is: Optional[str] = None
    keyspace_from_bundle: Optional[str] = None
    keyspace_from_arg: Optional[str] = None
    token_from_is: Optional[str] = None
    token_from_arg: Optional[str] = None
    #
    if auto:
        if any(
            v is not None
            for v in (
                session,
                secure_connect_bundle,
                init_string,
                token,
                database_id,
                keyspace,
                contact_points,
                username,
                password,
                cluster_kwargs,
            )
        ):
            raise ValueError("When auto=True, no arguments can be passed.")
        # setting some arguments from environment variables
        if "CASSANDRA_CONTACT_POINTS" in os.environ:
            contact_points = os.environ["CASSANDRA_CONTACT_POINTS"]
            username = os.environ.get("CASSANDRA_USERNAME")
            password = os.environ.get("CASSANDRA_PASSWORD")
            keyspace = os.environ.get("CASSANDRA_KEYSPACE")
        elif any(
            avar in os.environ
            for avar in [
                "ASTRA_DB_APPLICATION_TOKEN",
                "ASTRA_DB_INIT_STRING",
            ]
        ):
            token = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
            init_string = os.environ.get("ASTRA_DB_INIT_STRING")
            secure_connect_bundle = os.environ.get("ASTRA_DB_SECURE_BUNDLE_PATH")
            keyspace = os.environ.get("ASTRA_DB_KEYSPACE")
            database_id = os.environ.get("ASTRA_DB_DATABASE_ID")
    #
    try:
        # process init_string
        base_dir = tempfile_basedir if tempfile_basedir else tempfile.gettempdir()
        if init_string:
            temp_dir = tempfile.mkdtemp(dir=base_dir)
            temp_dir_created = True
            bundle_from_is, options_from_is = init_string_to_bundle_path_and_options(
                init_string,
                target_dir=temp_dir,
            )
            keyspace_from_is = options_from_is.get("keyspace")
            token_from_is = options_from_is.get("token")
        # for the session
        if session:
            direct_session = session
        if secure_connect_bundle:
            if not token:
                raise ValueError(
                    "`token` is required if `secure_connect_bundle` is passed"
                )
        # params from arguments:
        bundle_from_arg = secure_connect_bundle
        token_from_arg = token
        keyspace_from_arg = keyspace
        can_be_astra = any(
            [
                secure_connect_bundle is not None,
                init_string is not None,
                token is not None,
            ]
        )
        # resolution of priority among args
        if direct_session:
            default_session = direct_session
        else:
            # first determine if Cassandra or Astra
            is_cassandra = all(
                [
                    secure_connect_bundle is None,
                    init_string is None,
                    token is None,
                    contact_points is not None,
                ]
            )
            if is_cassandra:
                is_astra_db = False
            else:
                # determine if Astra DB
                is_astra_db = can_be_astra
            #
            if is_cassandra:
                # need to take care of:
                #   contact_points, username, password, cluster_kwargs
                chosen_contact_points: Union[List[str], None]
                if contact_points:
                    if isinstance(contact_points, str):
                        chosen_contact_points = [
                            cp.strip() for cp in contact_points.split(",") if cp.strip()
                        ]
                    else:
                        # assume it's a list already
                        chosen_contact_points = contact_points
                else:
                    # normalize "" to None for later `Cluster(...)` call
                    chosen_contact_points = None
                #
                if username is not None and password is not None:
                    chosen_auth_provider = PlainTextAuthProvider(
                        username,
                        password,
                    )
                else:
                    if username is not None or password is not None:
                        raise ValueError(
                            "Please provide both usename/password or none."
                        )
                    else:
                        chosen_auth_provider = None
                #
                if chosen_contact_points is None:
                    cluster = Cluster(
                        auth_provider=chosen_auth_provider,
                        **(cluster_kwargs if cluster_kwargs is not None else {})
                    )
                else:
                    cluster = Cluster(
                        contact_points=chosen_contact_points,
                        auth_provider=chosen_auth_provider,
                        **(cluster_kwargs if cluster_kwargs is not None else {})
                    )
                default_session = cluster.connect()
            elif is_astra_db:
                # Astra DB
                chosen_token = _first_valid(token_from_arg, token_from_is)
                if chosen_token is None:
                    raise ValueError(
                        "A token must be supplied if connection is to be established."
                    )
                chosen_bundle_pre_token = _first_valid(bundle_from_arg, bundle_from_is)
                # Try to get the bundle from the token if not supplied otherwise
                if chosen_bundle_pre_token is None:
                    if database_id is None:
                        raise ValueError(
                            "A database_id must be supplied if no "
                            "secure_connect_bundle is provided."
                        )
                    if not temp_dir_created:
                        temp_dir = tempfile.mkdtemp(dir=base_dir)
                        temp_dir_created = True
                    bundle_from_download = os.path.join(
                        temp_dir or "", DOWNLOADED_BUNDLE_FILE_NAME
                    )
                    download_astra_bundle_url(
                        database_id, chosen_token, bundle_from_download
                    )
                # After the devops-api part, re-evaluate chosen_bundle:
                chosen_bundle = _first_valid(
                    bundle_from_download, chosen_bundle_pre_token
                )
                #
                if chosen_bundle:
                    keyspace_from_bundle = infer_keyspace_from_bundle(chosen_bundle)
                    cluster = Cluster(
                        cloud={"secure_connect_bundle": chosen_bundle},
                        auth_provider=PlainTextAuthProvider(
                            ASTRA_CLOUD_AUTH_USERNAME,
                            chosen_token,
                        ),
                        **(cluster_kwargs if cluster_kwargs is not None else {})
                    )
                    default_session = cluster.connect()
                else:
                    raise ValueError("No secure-connect-bundle was available.")
        # keyspace to be resolved in any case
        chosen_keyspace = _first_valid(
            keyspace_from_arg, keyspace_from_is, keyspace_from_bundle
        )
        default_keyspace = chosen_keyspace
    finally:
        if temp_dir_created and temp_dir is not None:
            shutil.rmtree(temp_dir)


def resolve_session(arg_session: Optional[str] = None) -> Optional[Session]:
    """Utility to fall back to the global defaults when null args are passed."""
    if arg_session is not None:
        return arg_session
    else:
        return default_session


def check_resolve_session(arg_session: Optional[str] = None) -> Session:
    s = resolve_session(arg_session)
    if s is None:
        raise ValueError("DB session not set.")
    else:
        return s


def resolve_keyspace(arg_keyspace: Optional[str] = None) -> Optional[str]:
    """Utility to fall back to the global defaults when null args are passed."""
    if arg_keyspace is not None:
        return arg_keyspace
    else:
        return default_keyspace


def check_resolve_keyspace(arg_keyspace: Optional[str] = None) -> str:
    s = resolve_keyspace(arg_keyspace)
    if s is None:
        raise ValueError("DB keyspace not set.")
    else:
        return s


def _first_valid(*pargs: Optional[Any]) -> Union[Any, None]:
    for entry in pargs:
        if entry is not None:
            return entry
    return None
