"""
management of "history" of stored blobs, grouped
by some 'session id'. Overwrites are not supported by design.
"""

from cassandra.query import SimpleStatement

import cassio.cql


class StoredBlobHistory:

    def __init__(self, session, keyspace, table):
        self.session = session
        self.keyspace = keyspace
        self.table = table
        # Schema creation, if needed
        st = SimpleStatement(cassio.cql.create_session_table.format(
            keyspace=self.keyspace,
            table=self.table,
        ))
        session.execute(st)

    def store(self, session_id, blob, ttl_seconds):
        if ttl_seconds:
            ttl_spec = f' USING TTL {ttl_seconds}'
        else:
            ttl_spec = ''
        #
        st = SimpleStatement(cassio.cql.store_session_blob.format(
            keyspace=self.keyspace,
            table=self.table,
            ttlSpec=ttl_spec,
        ))
        self.session.execute(
            st,
            (session_id, blob,)
        )

    def retrieve(self, session_id, max_count=None):
        pass
        st = SimpleStatement(cassio.cql.get_session_blobs.format(
            keyspace=self.keyspace,
            table=self.table,
        ))
        rows = self.session.execute(
            st,
            (session_id,)
        )
        return (
            row.blob
            for row in rows
        )

    def clear_session_id(self, session_id):
        pass
        st = SimpleStatement(cassio.cql.clear_session.format(
            keyspace=self.keyspace,
            table=self.table,
        ))
        self.session.execute(st, (session_id,))
