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
package ai.langstream.cli.commands.profiles;

import ai.langstream.cli.LangStreamCLIConfig;
import ai.langstream.cli.NamedProfile;
import java.util.List;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "get", header = "Get an existing profile configuration")
public class GetProfileCmd extends BaseProfileCmd {

    @CommandLine.Parameters(description = "Name of the profile")
    private String name;

    @CommandLine.Option(
            names = {"-o"},
            description = "Output format. Formats are: yaml, json, raw. Default value is raw.")
    private Formats format = Formats.raw;

    @Override
    @SneakyThrows
    public void run() {
        ensureFormatIn(format, Formats.yaml, Formats.json, Formats.raw);
        checkGlobalFlags();
        final NamedProfile profile = getProfileOrThrow(name);
        final LangStreamCLIConfig config = getConfig();
        final Object object = format == Formats.raw ? List.of(profile) : profile;
        print(
                format,
                jsonPrinter.writeValueAsString(object),
                ListProfileCmd.COLUMNS_FOR_RAW,
                ListProfileCmd.rawMapping(config));
    }
}
