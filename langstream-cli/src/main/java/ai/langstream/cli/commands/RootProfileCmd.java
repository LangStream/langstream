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

import ai.langstream.cli.commands.profiles.CreateUpdateProfileCmd;
import ai.langstream.cli.commands.profiles.DeleteProfileCmd;
import ai.langstream.cli.commands.profiles.GetCurrentProfileCmd;
import ai.langstream.cli.commands.profiles.GetProfileCmd;
import ai.langstream.cli.commands.profiles.ImportProfileCmd;
import ai.langstream.cli.commands.profiles.ListProfileCmd;
import ai.langstream.cli.commands.profiles.SetCurrentProfileCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(
        name = "profiles",
        header = "Manage local profiles",
        subcommands = {
            CreateUpdateProfileCmd.CreateProfileCmd.class,
            CreateUpdateProfileCmd.UpdateProfileCmd.class,
            ListProfileCmd.class,
            GetCurrentProfileCmd.class,
            SetCurrentProfileCmd.class,
            GetProfileCmd.class,
            ImportProfileCmd.class,
            DeleteProfileCmd.class
        })
@Getter
public class RootProfileCmd {
    @CommandLine.ParentCommand private RootCmd rootCmd;
}
