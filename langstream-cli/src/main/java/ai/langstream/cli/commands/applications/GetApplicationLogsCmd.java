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

import ai.langstream.admin.client.AdminClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(
        name = "logs",
        mixinStandardHelpOptions = true,
        description = "Get LangStream application logs")
public class GetApplicationLogsCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(
            names = {"-f"},
            description = "Filter logs by the worker id")
    private List<String> filter;

    @Override
    @SneakyThrows
    public void run() {
        final String filterStr = filter == null ? "" : "?filter=" + String.join(",", filter);
        final AdminClient client = getClient();
        final HttpRequest request =
                client.newGet(client.tenantAppPath("/" + name + "/logs" + filterStr));
        final HttpResponse<InputStream> response =
                client.getHttpClient().send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            throw new RuntimeException(
                    "Failed to get application logs: "
                            + response.statusCode()
                            + " "
                            + new String(response.body().readAllBytes(), StandardCharsets.UTF_8));
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(response.body()));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                if ("Heartbeat".equals(line)) {
                    continue;
                }
                command.commandLine().getOut().println(line);
                command.commandLine().getOut().flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read logs", e);
        }
    }
}
