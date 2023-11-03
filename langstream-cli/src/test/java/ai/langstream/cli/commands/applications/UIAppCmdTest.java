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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UIAppCmdTest {

    static void writeExecutableFile(String fname, String contents) throws IOException {

        var path = Files.writeString(Path.of(fname), contents);

        Files.setPosixFilePermissions(
                path, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
    }

    @Test
    void openBrowser() throws IOException {
        // write test file
        String fname = "/tmp/" + UUID.randomUUID().toString();
        writeExecutableFile(fname, "#!/usr/bin/bash\necho hello\n");
        // ok
        String command = fname;
        boolean Result = UIAppCmd.checkAndLaunch(command, 80);
        assertTrue(Result, command + "\nif found in filesystem should be true");
        new File(fname).delete();
        // fail
        command = "_no_such_command_";
        Result = UIAppCmd.checkAndLaunch("_no_such_command_", 80);
        assertFalse(Result, command + "\nif not found in filesystemi should be false");
    }
}
