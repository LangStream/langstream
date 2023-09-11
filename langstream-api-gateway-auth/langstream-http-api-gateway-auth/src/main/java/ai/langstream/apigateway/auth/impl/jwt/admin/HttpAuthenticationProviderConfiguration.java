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
package ai.langstream.apigateway.auth.impl.jwt.admin;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HttpAuthenticationProviderConfiguration {

    @JsonAlias({"base-url", "baseurl"})
    private String baseUrl;

    @JsonAlias({"path-template", "pathtemplate"})
    private String pathTemplate;

    private Map<String, String> headers = new HashMap<>();

    @JsonAlias({"accepted-statuses", "acceptedstatuses"})
    private List<Integer> acceptedStatuses = List.of(200, 201);
}
