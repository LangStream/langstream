#
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

import time

from pulsar import (
    Client,
    ConsumerDeadLetterPolicy,
    ConsumerType
)

from _pulsar import ProducerConfiguration, ConsumerConfiguration, RegexSubscriptionMode

# Configuration Variables
service_url = 'pulsar://localhost:6650'
topic_name = 'persistent://public/default/my-topic'
subscription_name = 'my-subscription'
dlq_topic_name = 'persistent://public/default/my-topic-dlq'

# Create a client
client = Client(service_url)

# Create a producer
producer = client.create_producer(topic_name)

max_redeliver_count = 3

dlq_policy = ConsumerDeadLetterPolicy(max_redeliver_count=max_redeliver_count,  
                                     initial_subscription_name="dlq-listener")

# Create a consumer with a dead letter policy
consumer = client.subscribe(topic_name, subscription_name,
                            dead_letter_policy=dlq_policy,
                            negative_ack_redelivery_delay_ms=1000,  # Redelivery delay
                            consumer_type=ConsumerType.Shared)


# Sen num msgs.
producer = client.create_producer(topic_name)
properties = {
    'property_1': 'value_1',
    'property_2': 'value_2'
}
num = 10
for i in range(num):
    producer.send(("hello-%d" % i).encode('utf-8'), properties=properties)
producer.flush()

# Redeliver all messages maxRedeliverCountNum time.
for i in range(1, num * max_redeliver_count + num + 1):
    msg = consumer.receive()
    if i % num == 0:
        consumer.redeliver_unacknowledged_messages()
        print(f"Start redeliver msgs '{i}'")

# Messages should be in DLQ
try :
    consumer.receive(100)
except Exception as e:
    print(f"Exception: {e}")

print("Script completed.")
