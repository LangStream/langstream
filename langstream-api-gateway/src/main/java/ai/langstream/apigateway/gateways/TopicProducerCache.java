/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.apigateway.gateways;

import ai.langstream.api.runner.topics.TopicProducer;
import java.util.function.Supplier;

public interface TopicProducerCache {
    record Key(String tenant, String application, String gatewayId, String topic, String configString) {}

    TopicProducer getOrCreate(Key key, Supplier<TopicProducer> topicProducerSupplier);
}
