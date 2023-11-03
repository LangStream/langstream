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

import ai.langstream.cli.commands.configure.ConfigureCmd;
import lombok.Getter;
import picocli.AutoComplete;
import picocli.CommandLine;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        versionProvider = VersionProvider.class,
        scope = CommandLine.ScopeType.INHERIT,
        subcommands = {
            RootArchetypeCmd.class,
            RootAppCmd.class,
            ConfigureCmd.class,
            RootTenantCmd.class,
            RootGatewayCmd.class,
            RootProfileCmd.class,
            RootDockerCmd.class,
            RootPythonCmd.class,
            AutoComplete.GenerateCompletion.class
        })
public class RootCmd {

    @CommandLine.Option(
            names = {"--conf"},
            description = "LangStream CLI configuration file.")
    @Getter
    private String configPath;

    @CommandLine.Option(
            names = {"-p", "--profile"},
            description = "Profile to use with the command.")
    @Getter
    private String profile;

    @CommandLine.Option(
            names = {"-v", "--verbose"},
            defaultValue = "false",
            description = "Verbose mode. Helpful for troubleshooting.")
    @Getter
    private boolean verbose = false;

    @CommandLine.Option(
            names = {"--disable-local-repositories-cache"},
            defaultValue = "false",
            description =
                    "By default the repositories downloaded are cached. In case of corrupted directories, you might want to set add this parameter to always clone the repositories from scratch.")
    @Getter
    private boolean disableLocalRepositoriesCache = false;
}
