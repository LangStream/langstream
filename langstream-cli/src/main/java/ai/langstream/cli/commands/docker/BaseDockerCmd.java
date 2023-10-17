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
package ai.langstream.cli.commands.docker;

import ai.langstream.cli.commands.BaseCmd;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.commands.RootDockerCmd;
import picocli.CommandLine;

public abstract class BaseDockerCmd extends BaseCmd {

    @CommandLine.ParentCommand private RootDockerCmd rootDockerCmd;

    @Override
    protected RootCmd getRootCmd() {
        return rootDockerCmd.getRootCmd();
    }
}
