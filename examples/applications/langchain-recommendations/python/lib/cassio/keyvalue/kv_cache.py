"""
handling of key-value storage on a Cassandra table.
One row per partition, serializes a multiple partition key into a string
"""

from typing import Union, List, Any

from cassandra.cluster import Session

import cassio.cql


class KVCache:

    def __init__(self, session: Session, keyspace: str, table: str, keys: List[Any]):
        self.session = session
        self.keyspace = keyspace
        self.table = table
        self.keys = keys
        self.key_desc = '/'.join(self.keys)
        # Schema creation, if needed
        st = cassio.cql.create_kv_table.format(
            keyspace=self.keyspace,
            table=self.table,
        )
        session.execute(st)

    def clear(self):
        st = cassio.cql.truncate_table.format(
            keyspace=self.keyspace,
            table=self.table,
        )
        self.session.execute(st)

    def put(self, key_dict, cache_value, ttl_seconds):
        if ttl_seconds:
            ttl_spec = f' USING TTL {ttl_seconds}'
        else:
            ttl_spec = ''
        cache_key = self._serialize_key([
            key_dict[k]
            for k in self.keys
        ])
        st = cassio.cql.store_kv_item.format(
            keyspace=self.keyspace,
            table=self.table,
            ttlSpec=ttl_spec,
        )
        self.session.execute(
            st,
            (self.key_desc, cache_key, cache_value,),
        )

    def get(self, key_dict) -> Union[None, str]:
        cache_key = self._serialize_key([
            key_dict[k]
            for k in self.keys
        ])
        st = cassio.cql.get_kv_item.format(
            keyspace=self.keyspace,
            table=self.table,
        )
        row = self.session.execute(
            st,
            (self.key_desc, cache_key),
        ).one()
        if row:
            return row.cache_value
        else:
            return None

    def delete(self, key_dict) -> None:
        """ Will not complain if the row does not exist. """
        cache_key = self._serialize_key([
            key_dict[k]
            for k in self.keys
        ])
        st = cassio.cql.delete_kv_item.format(
            keyspace=self.keyspace,
            table=self.table,
        )
        self.session.execute(
            st,
            (self.key_desc, cache_key),
        )

    def _serialize_key(self, keys: List[str]):
        return str(keys)
