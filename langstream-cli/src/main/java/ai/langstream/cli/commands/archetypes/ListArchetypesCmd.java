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
package ai.langstream.cli.commands.archetypes;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.BiFunction;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list", header = "List all the available archetypes")
public class ListArchetypesCmd extends BaseArchetypeCmd {

    protected static final String[] COLUMNS_FOR_RAW = {"id", "labels"};

    @CommandLine.Option(
            names = {"-o"},
            description = "Output format. Formats are: yaml, json, raw. Default value is raw.")
    private Formats format = Formats.raw;

    @Override
    @SneakyThrows
    public void run() {
        ensureFormatIn(format, Formats.raw, Formats.json, Formats.yaml);
        final String body = getClient().archetypes().list();
        print(format, body, COLUMNS_FOR_RAW, getRawFormatValuesSupplier());
    }

    public static BiFunction<JsonNode, String, Object> getRawFormatValuesSupplier() {
        return (jsonNode, s) -> {
            switch (s) {
                case "id":
                    return searchValueInJson(jsonNode, "id");
                case "labels":
                    return searchValueInJson(jsonNode, "labels");
                default:
                    return jsonNode.get(s);
            }
        };
    }
}
