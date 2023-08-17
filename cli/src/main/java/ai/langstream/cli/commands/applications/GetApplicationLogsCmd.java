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

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "logs",
        description = "Get SGA application logs")
public class GetApplicationLogsCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(names = {"-f"}, description = "Filter logs by the worker id")
    private List<String> filter;

    @Override
    @SneakyThrows
    public void run() {
        final String filterStr = filter == null ? "" : "?filter=" + String.join(",", filter);
        getHttpClient().sendAsync(newGet(tenantAppPath("/" + name + "/logs" + filterStr)), HttpResponse.BodyHandlers.ofByteArrayConsumer(
                        new Consumer<Optional<byte[]>>() {
                            @Override
                            public void accept(Optional<byte[]> bytes) {
                                if (bytes.isPresent()) {
                                    command.commandLine().getOut().print(new String(bytes.get(), StandardCharsets.UTF_8));
                                    command.commandLine().getOut().flush();
                                }

                            }
                        }))
                .thenApply(HttpResponse::statusCode)
                .thenAccept((status) -> {
                    if (status != 200) {
                        err("ERROR: %d status received".formatted(status));
                    }
                }).join();


    }
}
