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
package ai.langstream.cli.commands;

import ai.langstream.cli.commands.archetypes.CreateAppFromArchetypeCmd;
import ai.langstream.cli.commands.archetypes.GetArchetypeCmd;
import ai.langstream.cli.commands.archetypes.ListArchetypesCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(
        name = "archetypes",
        header = "Use ${ROOT-COMMAND-NAME} Archetypes",
        subcommands = {
            ListArchetypesCmd.class,
            GetArchetypeCmd.class,
            CreateAppFromArchetypeCmd.class
        })
@Getter
public class RootArchetypeCmd {
    @CommandLine.ParentCommand private RootCmd rootCmd;
}
