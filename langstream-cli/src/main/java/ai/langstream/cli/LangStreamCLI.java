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
package ai.langstream.cli;

import ai.langstream.admin.client.HttpRequestFailedException;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.commands.VersionProvider;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import picocli.CommandLine;

public class LangStreamCLI {

    /**
     * @return ${LANGSTREAM_HOME} if set, otherwise ${user.home}/.langstream
     */
    @SneakyThrows
    public static Path getLangstreamCLIHomeDirectory() {
        final String langStreamHome = System.getProperty("LANGSTREAM_HOME");
        if (langStreamHome != null && !langStreamHome.isBlank()) {
            return Path.of(langStreamHome);
        }

        final String userHome = System.getProperty("user.home");
        if (!userHome.isBlank() && !"?".equals(userHome)) {
            final Path langstreamDir = Path.of(userHome, ".langstream");
            if (!Files.exists(langstreamDir)) {
                Files.createDirectories(langstreamDir);
            }
            return langstreamDir;
        }
        return null;
    }

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    public static int execute(String[] args) {
        String name = System.getenv("LANGSTREAM_CLI_CUSTOMIZE_NAME");
        String description = System.getenv("LANGSTREAM_CLI_CUSTOMIZE_DESCRIPTION");
        if (name == null) {
            name = "langstream";
        }
        if (description == null) {
            description = "LangStream CLI";
        }
        String version = System.getenv("LANGSTREAM_CLI_CUSTOMIZE_VERSION");
        VersionProvider.initialize(description, version);
        final CommandLine cmdLine = new CommandLine(new RootCmd());
        cmdLine.setCommandName(name);
        cmdLine.getCommandSpec().usageMessage().header(description);
        CommandLine gen = cmdLine.getSubcommands().get("generate-completion");
        gen.getCommandSpec().usageMessage().hidden(true);
        return cmdLine.setExecutionExceptionHandler(
                        (e, commandLine, parseResult) -> handleException(cmdLine, e, commandLine))
                .execute(args);
    }

    private static int handleException(CommandLine cmdLine, Exception e, CommandLine commandLine) {
        printExceptionMessage(computeErrorMessage(e), commandLine);
        RootCmd rootCmd = cmdLine.getCommand();
        if (rootCmd.isVerbose()) {
            e.printStackTrace(commandLine.getErr());
        }
        return 1;
    }

    private static String computeErrorMessage(Exception e) {
        final HttpRequestFailedException httpRequestFailedException =
                getHttpRequestFailedException(e);

        if (httpRequestFailedException != null) {
            String msg =
                    String.format(
                            "Http request to %s failed",
                            httpRequestFailedException.getRequest().uri());
            final HttpResponse<?> response = httpRequestFailedException.getResponse();
            if (response != null) {
                Object body = httpRequestFailedException.getResponse().body();
                if (body != null) {
                    if (body instanceof byte[]) {
                        body = new String((byte[]) body, StandardCharsets.UTF_8);
                    }
                    msg += String.format(": %s", body);
                } else {
                    msg += String.format(": %s", response.statusCode());
                }
            }
            return msg;

        } else if (e.getMessage() != null) {
            return e.getMessage();
        } else {
            return "Internal error: " + e;
        }
    }

    private static void printExceptionMessage(String message, CommandLine commandLine) {
        commandLine.getErr().println(commandLine.getColorScheme().errorText(message));
    }

    private static HttpRequestFailedException getHttpRequestFailedException(Throwable e) {

        if (e instanceof HttpRequestFailedException) {
            return (HttpRequestFailedException) e;
        }
        Throwable cause = e.getCause();
        while (true) {
            if (cause == null) {
                return null;
            }
            if (cause instanceof HttpRequestFailedException) {
                return (HttpRequestFailedException) cause;
            }
            cause = cause.getCause();
        }
    }
}
