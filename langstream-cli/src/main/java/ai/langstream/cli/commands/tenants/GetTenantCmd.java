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

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "get", header = "Get tenant configuration")
public class GetTenantCmd extends BaseTenantCmd {

    @CommandLine.Parameters(description = "Name of the tenant")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        final String body = getClient().http(getClient().newGet(pathForTenant(name))).body();
        log(body);
    }
}
