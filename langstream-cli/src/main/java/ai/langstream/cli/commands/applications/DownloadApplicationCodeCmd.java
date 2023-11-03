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

import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "download", header = "Get the application code")
public class DownloadApplicationCodeCmd extends BaseApplicationCmd {

    protected static final Pattern REGEX_PATTERN =
            Pattern.compile("filename=[\"']?([^\"'\r\n]+)[\"']?");

    @CommandLine.Parameters(description = "ID of the application")
    private String applicationId;

    @CommandLine.Option(
            names = {"-o", "--output-file"},
            description = "Output file")
    private String outputFile;

    @Override
    @SneakyThrows
    public void run() {
        final HttpResponse<byte[]> response = getClient().applications().download(applicationId);
        final Path path;
        if (outputFile != null) {
            path = Paths.get(outputFile);
        } else {
            String filename = null;
            final String contentDispositionValue =
                    response.headers().firstValue("Content-Disposition").orElse(null);
            if (contentDispositionValue != null) {

                Matcher matcher = REGEX_PATTERN.matcher(contentDispositionValue);
                if (matcher.find()) {
                    filename = matcher.group(1);
                }
            }
            if (filename == null) {
                filename =
                        String.format("%s-%s.zip", getCurrentProfile().getTenant(), applicationId);
            }
            path = Path.of(filename);
        }
        Files.write(path, response.body());
        log(String.format("Downloaded application code to %s", path.toAbsolutePath()));
    }
}
