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

import java.io.File;
import java.util.List;
import picocli.CommandLine;

@CommandLine.Command(
        name = "load-pip-requirements",
        header = "Process python dependencies in requirements.txt")
public class LoadPythonDependenciesCmd extends BasePythonCmd {

    protected void addSpecificCommand(List<String> commandLine) {
        commandLine.add(
                "pip3 install --target ./lib --upgrade --prefer-binary -r requirements.txt");
    }

    protected void validatePythonDirectory(File pythonDirectory) {
        File requirementsFile = new File(pythonDirectory, "requirements.txt");
        if (!requirementsFile.isFile()) {
            throw new IllegalArgumentException(
                    "File "
                            + requirementsFile.getAbsolutePath()
                            + " not found in "
                            + pythonDirectory);
        }
    }
}
