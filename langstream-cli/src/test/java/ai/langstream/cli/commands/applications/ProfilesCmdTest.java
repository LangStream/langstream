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
package ai.langstream.cli.commands.applications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.langstream.cli.NamedProfile;
import com.github.tomakehurst.wiremock.client.WireMock;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProfilesCmdTest extends CommandTestBase {

    @Test
    public void testCrud() {
        CommandResult result =
                executeCommand(
                        "profiles",
                        "create",
                        "new",
                        "--web-service-url",
                        "http://my.localhost:8080",
                        "--tenant",
                        "t",
                        "--token",
                        "tok",
                        "--api-gateway-url",
                        "http://my.localhost:8091");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new created", result.out());
        NamedProfile newProfile = getConfig().getProfiles().get("new");
        assertEquals("new", newProfile.getName());
        assertEquals("http://my.localhost:8080", newProfile.getWebServiceUrl());
        assertEquals("t", newProfile.getTenant());
        assertEquals("tok", newProfile.getToken());
        assertEquals("http://my.localhost:8091", newProfile.getApiGatewayUrl());

        result = executeCommand("profiles", "update", "new", "--token", "tok2");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new updated", result.out());
        newProfile = getConfig().getProfiles().get("new");
        assertEquals("new", newProfile.getName());
        assertEquals("http://my.localhost:8080", newProfile.getWebServiceUrl());
        assertEquals("t", newProfile.getTenant());
        assertEquals("tok2", newProfile.getToken());
        assertEquals("http://my.localhost:8091", newProfile.getApiGatewayUrl());

        result = executeCommand("profiles", "get", "new");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals(
                "PROFILE                   WEBSERVICEURL             TENANT                    TOKEN                 "
                        + "    CURRENT                 \n"
                        + "new                       http://my.localhost:8080  t                         ********",
                result.out());

        result = executeCommand("profiles", "get", "new", "-o", "json");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals(
                "{\n"
                        + "  \"webServiceUrl\" : \"http://my.localhost:8080\",\n"
                        + "  \"apiGatewayUrl\" : \"http://my.localhost:8091\",\n"
                        + "  \"tenant\" : \"t\",\n"
                        + "  \"token\" : \"tok2\",\n"
                        + "  \"name\" : \"new\"\n"
                        + "}",
                result.out());

        result = executeCommand("profiles", "get", "new", "-o", "yaml");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals(
                "---\n"
                        + "webServiceUrl: \"http://my.localhost:8080\"\n"
                        + "apiGatewayUrl: \"http://my.localhost:8091\"\n"
                        + "tenant: \"t\"\n"
                        + "token: \"tok2\"\n"
                        + "name: \"new\"",
                result.out());

        result = executeCommand("profiles", "get", "notexists");
        assertEquals(1, result.exitCode());
        assertEquals(
                "Profile notexists not found, maybe you meant one of these: default, new",
                result.err());
        assertEquals("", result.out());

        result = executeCommand("profiles", "list");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals(
                String.format(
                        ("PROFILE                   WEBSERVICEURL             TENANT                    TOKEN        "
                                + "        "
                                + "     CURRENT                 \n"
                                + "default                   %s    my-tenant                                           *      "
                                + "        "
                                + "         \n"
                                + "new                       http://my.localhost:8080  t                         ********"),
                        getConfig().getWebServiceUrl()),
                result.out());

        result = executeCommand("profiles", "get-current");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("default", result.out());

        result = executeCommand("profiles", "set-current", "new");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new set as current", result.out());
        assertEquals("new", getConfig().getCurrentProfile());

        result = executeCommand("profiles", "delete", "new");
        assertEquals(1, result.exitCode());
        assertEquals("Cannot delete the current profile", result.err());
        assertEquals("", result.out());
        assertEquals("new", getConfig().getCurrentProfile());

        result = executeCommand("profiles", "set-current", "default");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile default set as current", result.out());
        assertEquals("default", getConfig().getCurrentProfile());

        result = executeCommand("profiles", "delete", "new");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new deleted", result.out());
        assertEquals("default", getConfig().getCurrentProfile());
        assertNull(getConfig().getProfiles().get("new"));
    }

    @Test
    public void testCreateAndSet() {
        CommandResult result =
                executeCommand(
                        "profiles",
                        "create",
                        "new",
                        "--web-service-url",
                        "http://my.localhost:8080",
                        "--set-current");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new created\n" + "profile new set as current", result.out());
        assertEquals("new", getConfig().getCurrentProfile());

        result =
                executeCommand(
                        "profiles",
                        "create",
                        "new1",
                        "--web-service-url",
                        "http://my.localhost:8080");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new1 created", result.out());
        assertEquals("new", getConfig().getCurrentProfile());

        result =
                executeCommand(
                        "profiles",
                        "update",
                        "new1",
                        "--web-service-url",
                        "http://my.localhost:8080",
                        "--set-current");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new1 updated\n" + "profile new1 set as current", result.out());
        assertEquals("new1", getConfig().getCurrentProfile());
    }

    @ParameterizedTest
    @ValueSource(strings = {"file", "json", "base64"})
    public void testImport(String input) throws IOException {

        final String json =
                "{\"webServiceUrl\":\"http://my.localhost:8080\",\"apiGatewayUrl\":\"http://my.localhost:8091\","
                        + "\"tenant\":\"t\",\"token\":\"tok\"}";
        final String paramName;

        switch (input) {
            case "file":
                paramName = "--file";
                break;
            case "json":
            case "base64":
                paramName = "--inline";
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + input);
        }

        final File file = Files.createTempFile("test", ".json").toFile();
        Files.writeString(file.toPath(), json);
        final String paramValue;
        switch (input) {
            case "file":
                paramValue = file.getAbsolutePath();
                break;
            case "json":
                paramValue = json;
                break;
            case "base64":
                paramValue = "base64:" + Base64.getEncoder().encodeToString(json.getBytes());
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + input);
        }
        ;

        CommandResult result =
                executeCommand("profiles", "import", "new", paramName, paramValue, "--set-current");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new created\n" + "profile new set as current", result.out());
        assertEquals("new", getConfig().getCurrentProfile());

        NamedProfile newProfile = getConfig().getProfiles().get("new");
        assertEquals("new", newProfile.getName());
        assertEquals("http://my.localhost:8080", newProfile.getWebServiceUrl());
        assertEquals("t", newProfile.getTenant());
        assertEquals("tok", newProfile.getToken());
        assertEquals("http://my.localhost:8091", newProfile.getApiGatewayUrl());

        result =
                executeCommand("profiles", "import", "new", paramName, paramValue, "--set-current");
        assertEquals(1, result.exitCode());
        assertEquals("Profile new already exists", result.err());
        assertEquals("", result.out());

        result =
                executeCommand(
                        "profiles", "import", "new", paramName, paramValue, "--set-current", "-u");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new updated\n" + "profile new set as current", result.out());
    }

    @Test
    public void testDefaultProfile() {

        CommandResult result = executeCommand("profiles", "get", "default");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals(
                String.format(
                        ("PROFILE                 WEBSERVICEURL           TENANT                  TOKEN              "
                                + "     "
                                + "CURRENT               \n"
                                + "default                 %s  my-tenant                                       *"),
                        getConfig().getWebServiceUrl()),
                result.out());

        result = executeCommand("profiles", "get-current");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("default", result.out());

        result = executeCommand("profiles", "create", "default");
        assertEquals(1, result.exitCode());
        assertEquals("Profile default already exists", result.err());
        assertEquals("", result.out());

        result =
                executeCommand(
                        "profiles",
                        "update",
                        "default",
                        "--token",
                        "tok2",
                        "--web-service-url",
                        "http://my.localhost:8080",
                        "--api-gateway-url",
                        "ws://my.localhost:8091");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile default updated", result.out());
        assertEquals("tok2", getConfig().getToken());
        assertEquals("my-tenant", getConfig().getTenant());
        assertEquals("http://my.localhost:8080", getConfig().getWebServiceUrl());
        assertEquals("ws://my.localhost:8091", getConfig().getApiGatewayUrl());

        result =
                executeCommand(
                        "profiles",
                        "import",
                        "default",
                        "--inline",
                        "{\"webServiceUrl\":\"http://my0.localhost/ls\",\"tenant\":\"a-new\"}",
                        "-u");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile default updated", result.out());
        assertEquals("a-new", getConfig().getTenant());
        assertEquals("http://my0.localhost/ls", getConfig().getWebServiceUrl());
        assertNull(getConfig().getToken());
        assertNull(getConfig().getApiGatewayUrl());

        result = executeCommand("profiles", "delete", "default");
        assertEquals(1, result.exitCode());
        assertEquals("Profile name default can't be deleted", result.err());
        assertEquals("", result.out());
    }

    @Test
    public void testGlobalParam() {
        wireMock.register(
                WireMock.get("/api/applications/" + TENANT).willReturn(WireMock.ok("[]")));

        CommandResult result = executeCommand("apps", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("[ ]", result.out());

        result =
                executeCommand(
                        "profiles",
                        "create",
                        "new",
                        "--web-service-url",
                        "http://my.localhost:8080",
                        "--set-current");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("profile new created\n" + "profile new set as current", result.out());

        result = executeCommand("apps", "list", "-o", "json");
        assertEquals(1, result.exitCode());
        assertEquals("Tenant not set. Please set the tenant in the configuration.", result.err());
        assertEquals("", result.out());

        result = executeCommand("-p", "default", "apps", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        assertEquals("", result.err());
        assertEquals("[ ]", result.out());

        result = executeCommand("-p", "notexists", "apps", "list", "-o", "json");
        assertEquals(1, result.exitCode());
        assertEquals("No profile 'notexists' defined in configuration", result.err());
        assertEquals("", result.out());
    }
}
