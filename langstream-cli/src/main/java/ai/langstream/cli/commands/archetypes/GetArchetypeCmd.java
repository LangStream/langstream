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
import lombok.SneakyThrows;
import picocli.CommandLine;

import java.util.function.BiFunction;

@CommandLine.Command(name = "get", header = "Get the metadata about an archetype")
public class GetArchetypeCmd extends BaseArchetypeCmd {

    @CommandLine.Option(
            names = {"-a", "--archetype"},
            description = "Id of the archetype to get")
    private String archetypeId;

    @Override
    @SneakyThrows
    public void run() {
        final String body = getClient().archetypes().get(archetypeId);
        log(body);
    }

}
