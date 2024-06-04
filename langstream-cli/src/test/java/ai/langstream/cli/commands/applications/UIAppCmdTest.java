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
package ai.langstream.cli.commands.applications;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import org.junit.jupiter.api.Test;

class UIAppCmdTest {

    @Test
    void openBrowser() throws Exception {
        Path outputFile = Files.createTempFile("langstream", ".txt");
        Path commandFile = Files.createTempFile("langstream", ".sh");
        Files.writeString(
                commandFile, "#!/bin/bash\necho $1 > " + outputFile.toFile().getAbsolutePath());
        String filePath = commandFile.toFile().getAbsolutePath();
        Files.setPosixFilePermissions(
                commandFile,
                Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
        boolean result = UIAppCmd.runCommand(filePath, "http://localhost:9999");
        assertTrue(result);
        assertEquals("http://localhost:9999\n", Files.readString(outputFile));
        assertFalse(UIAppCmd.runCommand("_no_such_command_", ""));
    }
}
