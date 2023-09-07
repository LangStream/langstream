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
package ai.langstream.cli.commands.tenants;

import java.net.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "create", header = "Create a new tenant")
public class CreateTenantCmd extends BaseTenantCmd {

    @CommandLine.Parameters(description = "Name of the tenant")
    private String name;

    @CommandLine.Option(
            names = {"--max-total-resource-units"},
            description = "Max application's units for the tenant.")
    private Integer maxUnits;

    @Override
    @SneakyThrows
    public void run() {
        Map<String, Object> request = new HashMap<>();
        request.put("maxTotalResourceUnits", maxUnits);
        final String body = jsonBodyWriter.writeValueAsString(request);

        final HttpRequest httpRequest =
                getClient()
                        .newPost(
                                pathForTenant(name),
                                "application/json",
                                HttpRequest.BodyPublishers.ofString(body));
        getClient().http(httpRequest);
        log(String.format("tenant %s created", name));
    }
}
