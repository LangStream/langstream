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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.http.HttpResponse;
import java.util.List;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "logs", header = "Get the application logs")
public class GetApplicationLogsCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "ID of the application")
    private String applicationId;

    @CommandLine.Option(
            names = {"-f"},
            description = "Filter logs by the worker id")
    private List<String> filter;

    @CommandLine.Option(
            names = {"-o"},
            description = "Logs format. Supported values: \"text\" and \"json\". Default: \"text\"")
    private String format = "text";

    @Override
    @SneakyThrows
    public void run() {
        final HttpResponse<InputStream> response =
                getClient().applications().logs(applicationId, filter, format);

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
