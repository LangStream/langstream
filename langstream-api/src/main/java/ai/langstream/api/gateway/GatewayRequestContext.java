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
package ai.langstream.api.gateway;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import java.util.Map;

public interface GatewayRequestContext {

    String tenant();

    String applicationId();

    Application application();

    Gateway gateway();

    String credentials();

    boolean isTestMode();

    Map<String, String> userParameters();

    Map<String, String> options();

    Map<String, String> httpHeaders();
}
