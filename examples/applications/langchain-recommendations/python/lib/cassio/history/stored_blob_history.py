"""
management of "history" of stored blobs, grouped
by some 'session id'. Overwrites are not supported by design.
"""

import uuid
from warnings import warn
from typing import Any, Dict, Iterable, Optional

from cassandra.cluster import Session  # type: ignore

from cassio.table.tables import ClusteredCassandraTable


class StoredBlobHistory:
    """
    This class is a rewriting of the StoredBlobHistory created for use in LangChain
    integration, this time relying on the class-table-hierarchy (cassio.table.*).

    It mostly provides a translation layer between parameters and key names,
    using a clustered table class internally.

    Additional kwargs, for use in this new table class, are passed as they are
    in order to enable their usage already before adapting the LangChain
    integration code.
    """

    DEPRECATION_MESSAGE = (
        "Class `StoredBlobHistory` is a legacy construct and "
        "will be deprecated in future versions of CassIO."
    )

    def __init__(
        self, session: Session, keyspace: str, table_name: str, **kwargs: Dict[str, Any]
    ):
        #
        warn(self.DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        # specifications are added such as the type of the row_id
        full_kwargs = {
            **kwargs,
            **{
                "primary_key_type": ["TEXT", "TIMEUUID"],
                # latest entries are returned first
                "ordering_in_partition": "DESC",
            },
        }
        self.table = ClusteredCassandraTable(
            session=session,
            keyspace=keyspace,
            table=table_name,
            **full_kwargs,
        )

    def store(self, session_id: str, blob: str, ttl_seconds: Optional[int]) -> None:
        this_row_id = uuid.uuid1()
        self.table.put(
            partition_id=session_id,
            row_id=this_row_id,
            body_blob=blob,
            ttl_seconds=ttl_seconds,
        )

    def retrieve(
        self, session_id: str, max_count: Optional[int] = None
    ) -> Iterable[str]:
        # The latest are returned, in chronological order
        return [
            row["body_blob"]
            for row in self.table.get_partition(
                partition_id=session_id,
                n=max_count,
            )
        ][::-1]

    def clear_session_id(self, session_id: str) -> None:
        self.table.delete_partition(session_id)
        return None
