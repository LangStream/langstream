"""
Inspection of a keyspace and its tables through metadata in the Session object.
"""

from typing import Iterable, Tuple

from cassandra.cluster import Session  # type: ignore

ColumnType = Tuple[str, str]


def table_primarykey(
    session: Session, keyspace: str, table: str
) -> Iterable[ColumnType]:
    table_obj = session.cluster.metadata.keyspaces[keyspace].tables[table]
    return ((col.name, col.cql_type) for col in table_obj.partition_key)


def table_clusteringcolumns(
    session: Session, keyspace: str, table: str
) -> Iterable[ColumnType]:
    table_obj = session.cluster.metadata.keyspaces[keyspace].tables[table]
    return ((col.name, col.cql_type) for col in table_obj.clustering_key)


def table_partitionkey(
    session: Session, keyspace: str, table: str
) -> Iterable[ColumnType]:
    return (
        col
        for col_src in (
            table_primarykey(session, keyspace, table),
            table_clusteringcolumns(session, keyspace, table),
        )
        for col in col_src
    )
