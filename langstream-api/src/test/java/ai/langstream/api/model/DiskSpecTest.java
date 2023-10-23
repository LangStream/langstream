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
package ai.langstream.api.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class DiskSpecTest {

    @Test
    void testParseSiskSpace() {
        assertEquals(0, DiskSpec.parseSize("0"));
        assertEquals(0, DiskSpec.parseSize("0G"));
        assertEquals(0, DiskSpec.parseSize("0M"));
        assertEquals(0, DiskSpec.parseSize("0K"));
        assertEquals(1024L, DiskSpec.parseSize("1K"));
        assertEquals(1024L * 1024L, DiskSpec.parseSize("1M"));
        assertEquals(1024L * 1024L * 1024L, DiskSpec.parseSize("1G"));
        assertEquals(256L * 1024L * 1024L, DiskSpec.parseSize("256M"));

        // remove spaces
        assertEquals(1024L * 1024L * 1024L, DiskSpec.parseSize(" 1  G"));
        assertEquals(1024L * 1024L * 1024L, DiskSpec.parseSize("1\t G "));
    }
}
