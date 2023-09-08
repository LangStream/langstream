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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.cli.commands.GitIgnoreParser;
import java.io.IOException;
import java.nio.file.Paths;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class GitIgnoreParserTest {

    @ParameterizedTest
    @CsvSource({
        "src/test/resources/aaa, false, true",
        "src/test/resources/a/aaa, false, true",
        "src/test/resources/a/a/aaa, false, true",
        "src/test/resources/#comment, false, false",
        "src/test/resources/#, false, true",
        "src/test/resources/bbb, true, true",
        "src/test/resources/b/bbb, true, true",
        "src/test/resources/bbb, false, false",
        "src/test/resources/b/bbb, false, false",
        "src/test/resources/ccc, false, true",
        "src/test/resources/c/ccc, false, false",
        "src/test/resources/ddd/eee, false, true",
        "src/test/resources/d/ddd/eee, false, false",
        "src/test/resources/fff/ggg, true, true",
        "src/test/resources/fff/ggg, false, false",
        "src/test/resources/hhh/h, false, true",
        "src/test/resources/hhh/hh, false, false",
        "src/test/resources/i/iii, false, true",
        "src/test/resources/i/i/iii, false, false",
        "src/test/resources/jjj/j/kkk, false, true",
        "src/test/resources/jjj/j/k/kkk, false, false",
        "src/test/resources/lllm, false, true",
        "src/test/resources/lll, false, true",
        "src/test/resources/lllmm, false, true",
        "src/test/resources/lll/m, false, false",
        "src/test/resources/mmml, false, true",
        "src/test/resources/mmm, false, false",
        "src/test/resources/mmmll, false, false",
        "src/test/resources/mmm/l, false, false",
        "src/test/resources/nnnsss, false, false",
        "src/test/resources/nnnosss, false, true",
        "src/test/resources/nnnpsss, false, true",
        "src/test/resources/nnnqsss, false, true",
        "src/test/resources/nnnrsss, false, true",
        "src/test/resources/ttt, false, true",
        "src/test/resources/t/ttt, false, true",
        "src/test/resources/t/t/ttt, false, true",
        "src/test/resources/uuu, false, false",
        "src/test/resources/uuu, true, false",
        "src/test/resources/uuu/u, false, true",
        "src/test/resources/uuu/u/u, false, true",
        "src/test/resources/vvv/www, false, false",
        "src/test/resources/vvv/v/www, false, true",
        "src/test/resources/vvv/v/v/www, false, true",
    })
    void testParser(String path, boolean isDir, boolean expectedMatch) throws IOException {
        GitIgnoreParser parser =
                new GitIgnoreParser(Paths.get("src", "test", "resources", ".langstreamignore"));
        assertEquals(expectedMatch, parser.matches(path, isDir));
    }
}
