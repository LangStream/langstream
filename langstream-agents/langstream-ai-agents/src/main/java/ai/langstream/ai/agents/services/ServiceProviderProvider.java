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
package ai.langstream.ai.agents.services;

import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import java.util.Map;

public interface ServiceProviderProvider {

    boolean supports(Map<String, Object> agentConfiguration);

    ServiceProvider createImplementation(
            Map<String, Object> agentConfiguration, MetricsReporter metricsReporter);
}
