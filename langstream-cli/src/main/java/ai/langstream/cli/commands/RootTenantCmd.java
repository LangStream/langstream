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

import ai.langstream.cli.commands.tenants.CreateTenantCmd;
import ai.langstream.cli.commands.tenants.DeleteTenantCmd;
import ai.langstream.cli.commands.tenants.GetTenantCmd;
import ai.langstream.cli.commands.tenants.ListTenantCmd;
import ai.langstream.cli.commands.tenants.PutTenantCmd;
import ai.langstream.cli.commands.tenants.UpdateTenantCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(
        name = "tenants",
        header = "Manage ${ROOT-COMMAND-NAME} tenants",
        subcommands = {
            PutTenantCmd.class,
            DeleteTenantCmd.class,
            ListTenantCmd.class,
            GetTenantCmd.class,
            CreateTenantCmd.class,
            UpdateTenantCmd.class
        })
@Getter
public class RootTenantCmd {
    @CommandLine.ParentCommand private RootCmd rootCmd;
}
