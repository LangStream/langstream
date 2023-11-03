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

import org.junit.jupiter.api.Test;

class UIAppCmdTest {

    @Test
    void openBrowser() {
        // ok
        String command = "/usr/bin/echo";
        boolean Result = UIAppCmd.checkAndLaunch(command, 80);
        assertTrue(Result, command + "if found in filesystem should be true");
        // fail
        command = "_no_such_command_";
        Result = UIAppCmd.checkAndLaunch("_no_such_command_", 80);
        assertFalse(Result, command + "if not found in filesystemi should be false");
    }
}
