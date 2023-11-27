"""
handling of key-value storage on a Cassandra table.
One row per partition, serializes a multiple partition key into a string
"""

from warnings import warn
from typing import Any, cast, Dict, List, Optional

from cassandra.cluster import Session  # type: ignore

from cassio.table.tables import ElasticCassandraTable


class KVCache:
    """
    This class is a rewriting of the KVCache created for use in LangChain
    integration, this time relying on the class-table-hierarchy (cassio.table.*).

    It mostly provides a translation layer between parameters and key names,
    using a clustered table class internally.

    Additional kwargs, for use in this new table class, are passed as they are
    in order to enable their usage already before adapting the LangChain
    integration code.
    """

    DEPRECATION_MESSAGE = (
        "Class `KVCache` is a legacy construct and "
        "will be deprecated in future versions of CassIO."
    )

    def __init__(
        self,
        table: str,
        keys: List[Any],
        session: Optional[Session] = None,
        keyspace: Optional[str] = None,
    ):
        #
        warn(self.DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        # for LangChain this is what we expect - no other uses are planned:
        assert all(isinstance(k, str) for k in keys)
        p_k_type = ["TEXT"] * len(keys)
        #
        self.table = ElasticCassandraTable(
            session,
            keyspace,
            table,
            keys=keys,
            primary_key_type=p_k_type,
        )

    def clear(self) -> None:
        self.table.clear()
        return None

    def put(
        self,
        key_dict: Dict[str, str],
        cache_value: str,
        ttl_seconds: Optional[int] = None,
    ) -> None:
        self.table.put(body_blob=cache_value, ttl_seconds=ttl_seconds, **key_dict)
        return None

    def get(self, key_dict: Dict[str, Any]) -> Optional[str]:
        entry = self.table.get(**key_dict)
        if entry is None:
            return None
        else:
            return cast(str, entry["body_blob"])

    def delete(self, key_dict: Dict[str, Any]) -> None:
        """Will not complain if the row does not exist."""
        self.table.delete(**key_dict)
        return None
