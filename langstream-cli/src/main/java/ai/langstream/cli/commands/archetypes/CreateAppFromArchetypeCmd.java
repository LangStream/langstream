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

import ai.langstream.admin.client.AdminClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;

@CommandLine.Command(name = "create-application", header = "Create an application from an archetype")
public class CreateAppFromArchetypeCmd extends BaseArchetypeCmd {

    private static final ObjectMapper mapper = new ObjectMapper();

    @CommandLine.Parameters(description = "Name of the application")
    private String name;
    @CommandLine.Option(
            names = {"-a", "--archetype"},
            description = "Id of the archetype to use")
    private String archetypeId;

    @CommandLine.Option(
            names = {"-p", "--parameters-file"},
            description = "File that contains the parameters to use for the application")
    private String parametersFile;

    @CommandLine.Option(
            names = {"--dry-run"},
            description =
                    "Dry-run mode. Do not deploy the application but only resolves placeholders and display the result.")
    private boolean dryRun;

    @CommandLine.Option(
            names = {"-o"},
            description =
                    "Output format for dry-run mode. Formats are: yaml, json. Default value is yaml.")
    private Formats format = Formats.yaml;

    Formats format() {
        ensureFormatIn(format, Formats.json, Formats.yaml);
        return format;
    }

    @Override
    @SneakyThrows
    public void run() {
       Map<String, Object> parameters = readParameters(parametersFile);
        AdminClient client = getClient();
        // verify archetype exists
        client.archetypes().get(archetypeId);

        final String response = client.applications().deployFromArchetype(name, archetypeId, parameters, dryRun);
        if (dryRun) {
            final Formats format = format();
            print(format == Formats.raw ? Formats.yaml : format, response, null, null);
        } else {
            log(String.format("application %s deployed", name));
        }
    }

    private Map<String, Object> readParameters(String parametersFile) {
        final File file = new File(parametersFile);
        if (!file.exists()) {
            throw new IllegalArgumentException("File " + parametersFile + " does not exist");
        }
        try {
            return mapper.readValue(file, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException err) {
            throw new IllegalArgumentException("Cannot parse parameters file " + file.getAbsolutePath()+": " + err.getMessage(), err);
        }
    }

}
