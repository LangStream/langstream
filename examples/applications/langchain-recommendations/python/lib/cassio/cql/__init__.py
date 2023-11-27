create_vector_table = """
CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
    document_id {idType} PRIMARY KEY,
    embedding_vector VECTOR<FLOAT, {embeddingDimension}>,
    document TEXT,
    metadata_blob TEXT
)
"""
create_vector_table_index = """
CREATE CUSTOM INDEX IF NOT EXISTS {indexName} ON {keyspace}.{table} (embedding_vector)
USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' ;
"""
store_cached_vss_item = """
INSERT INTO {keyspace}.{table} (
    document_id,
    embedding_vector,
    document,
    metadata_blob
) VALUES (
    {documentIdPlaceholder},
    %s,
    %s,
    %s
){ttlSpec}
"""
get_vector_table_item = """
SELECT
    document_id, embedding_vector, document, metadata_blob
FROM {keyspace}.{table}
    WHERE document_id=%s
"""
search_vector_table_item = """
SELECT
    document_id, embedding_vector, document, metadata_blob
FROM {keyspace}.{table}
    ORDER BY embedding_vector ANN OF %s
    LIMIT %s
    ALLOW FILTERING
"""
truncate_vector_table = """
TRUNCATE TABLE {keyspace}.{table};
"""
delete_vector_table_item = """
DELETE FROM {keyspace}.{table}
WHERE document_id = %s
"""
count_rows = """
    SELECT COUNT(*) FROM {keyspace}.{table}
"""
retrieve_one_row = 'SELECT * FROM {keyspace}.{table} WHERE {whereClause} LIMIT 1'
create_session_table = """
CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
    session_id TEXT,
    blob_id TIMEUUID,
    blob TEXT,
    PRIMARY KEY (( session_id ) , blob_id )
) WITH CLUSTERING ORDER BY (blob_id ASC)
"""
get_session_blobs = """
SELECT blob
    FROM {keyspace}.{table}
WHERE session_id=%s
"""
store_session_blob = """
INSERT INTO {keyspace}.{table} (
    session_id,
    blob_id,
    blob
) VALUES (
    %s,
    now(),
    %s
){ttlSpec}
"""
clear_session = """
DELETE FROM {keyspace}.{table} WHERE session_id = %s
"""
create_kv_table = """
CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
    key_desc TEXT,
    cache_key TEXT,
    cache_value TEXT,
    PRIMARY KEY (( key_desc, cache_key ))
)
"""
get_kv_item = """
SELECT cache_value
    FROM {keyspace}.{table}
WHERE key_desc=%s
    AND cache_key=%s
"""
delete_kv_item = """
DELETE FROM {keyspace}.{table}
WHERE key_desc=%s
    AND cache_key=%s
"""
store_kv_item = """
INSERT INTO {keyspace}.{table} (
    key_desc,
    cache_key,
    cache_value
) VALUES (
    %s,
    %s,
    %s
){ttlSpec}
"""
truncate_table = """
TRUNCATE TABLE {keyspace}.{table}
"""
