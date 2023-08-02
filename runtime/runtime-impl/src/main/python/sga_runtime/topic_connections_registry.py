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
#
from . import kafka_connection

TOPIC_CONNECTIONS_RUNTIME = {
    'kafka': kafka_connection
}


def get_topic_connections_runtime(streaming_cluster):
    if 'type' not in streaming_cluster:
        raise ValueError('streamingCluster type cannot be null')

    streaming_cluster_type = streaming_cluster['type']

    if streaming_cluster_type not in TOPIC_CONNECTIONS_RUNTIME:
        raise ValueError(f'No topic connectionImplementation found for type {streaming_cluster_type}')
    return TOPIC_CONNECTIONS_RUNTIME[streaming_cluster_type]
