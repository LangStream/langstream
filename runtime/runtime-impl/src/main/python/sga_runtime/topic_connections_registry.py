import kafka_topic_connections_runtime

TOPIC_CONNECTIONS_RUNTIME = {
    'kafka': kafka_topic_connections_runtime
}


def get_topic_connections_runtime(streaming_cluster):
    if 'type' not in streaming_cluster:
        raise ValueError('streamingCluster type cannot be null')

    streaming_cluster_type = streaming_cluster['type']

    if streaming_cluster_type not in TOPIC_CONNECTIONS_RUNTIME:
        raise ValueError(f'No TopicConnectionsRuntimeProvider found for type {streaming_cluster_type}')
    return TOPIC_CONNECTIONS_RUNTIME[streaming_cluster_type]


