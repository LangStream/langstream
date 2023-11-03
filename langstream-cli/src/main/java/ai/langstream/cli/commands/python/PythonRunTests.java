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
package ai.langstream.cli.commands.python;

import java.util.List;
import picocli.CommandLine;

@CommandLine.Command(name = "run-tests", header = "Execute python tests")
public class PythonRunTests extends BasePythonCmd {

    @CommandLine.Option(
            names = {"-c", "--command"},
            description = "Application directory path")
    protected String command = "python3 -m unittest";

    protected void addSpecificCommand(List<String> commandLine) {
        log("Running tests");
        log("Command is");
        log(command);
        commandLine.add("PYTHONPATH=$PYTHONPATH:/app-code-download/python/lib " + command);
    }
}
