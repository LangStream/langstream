/**
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
package ai.langstream.cli.commands.applications;

import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class TenantsCmdTest extends CommandTestBase {


    @Test
    public void testPut() throws Exception {
        wireMock.register(WireMock.put("/api/tenants/%s".formatted("newt")).willReturn(WireMock.ok("{ \"name\": \"newt\" }")));
        CommandResult result = executeCommand("tenants", "put", "newt");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("tenant newt created/updated", result.out());

    }


    @Test
    public void testGet() throws Exception {
        wireMock.register(WireMock.get("/api/tenants/%s".formatted("newt")).willReturn(WireMock.ok("{ \"name\": \"newt\" }")));
        CommandResult result = executeCommand("tenants", "get", "newt");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("{ \"name\": \"newt\" }", result.out());

    }

    @Test
    public void testDelete() throws Exception {
        wireMock.register(WireMock.delete("/api/tenants/%s"
                .formatted("newt")).willReturn(WireMock.ok()));

        CommandResult result = executeCommand("tenants", "delete", "newt");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Tenant newt deleted", result.out());

    }

    @Test
    public void testList() throws Exception {
        wireMock.register(WireMock.get("/api/tenants"
                .formatted(TENANT)).willReturn(WireMock.ok("{}")));

        CommandResult result = executeCommand("tenants", "list");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("{}", result.out());

    }
}
