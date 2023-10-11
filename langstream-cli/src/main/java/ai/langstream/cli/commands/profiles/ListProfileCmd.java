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
import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.BiFunction;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list", header = "List profiles")
public class ListProfileCmd extends BaseProfileCmd {

    static final String[] COLUMNS_FOR_RAW = {
        "profile", "webServiceUrl", "tenant", "token", "current"
    };

    @CommandLine.Option(
            names = {"-o"},
            description = "Output format. Formats are: yaml, json, raw. Default value is raw.")
    private Formats format = Formats.raw;

    @Override
    @SneakyThrows
    public void run() {
        ensureFormatIn(format, Formats.raw, Formats.json, Formats.yaml);
        checkGlobalFlags();
        final LangStreamCLIConfig config = getConfig();
        final Object object = format == Formats.raw ? listAllProfiles() : config;

        print(format, jsonPrinter.writeValueAsString(object), COLUMNS_FOR_RAW, rawMapping(config));
    }

    static BiFunction<JsonNode, String, Object> rawMapping(LangStreamCLIConfig config) {
        return (jsonNode, s) -> {
            switch (s) {
                case "profile":
                    return searchValueInJson(jsonNode, "name");
                case "webServiceUrl":
                    return searchValueInJson(jsonNode, "webServiceUrl");
                case "tenant":
                    return searchValueInJson(jsonNode, "tenant");
                case "token":
                    final Object token = searchValueInJson(jsonNode, "token");
                    return token == null ? null : "********";
                case "current":
                    final Object name = searchValueInJson(jsonNode, "name");
                    return name.equals(config.getCurrentProfile()) ? "*" : "";
                default:
                    return null;
            }
        };
    }
}
