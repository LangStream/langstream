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

import java.util.Map;

public interface GatewayAuthenticationResult {

    static GatewayAuthenticationResult authenticationSuccessful(
            Map<String, String> principalValues) {
        return new GatewayAuthenticationResult() {
            @Override
            public boolean authenticated() {
                return true;
            }

            @Override
            public String reason() {
                return null;
            }

            @Override
            public Map<String, String> principalValues() {
                return principalValues;
            }
        };
    }

    static GatewayAuthenticationResult authenticationFailed(String reason) {
        return new GatewayAuthenticationResult() {
            @Override
            public boolean authenticated() {
                return false;
            }

            @Override
            public String reason() {
                return reason;
            }

            @Override
            public Map<String, String> principalValues() {
                return null;
            }
        };
    }

    boolean authenticated();

    String reason();

    Map<String, String> principalValues();
}
