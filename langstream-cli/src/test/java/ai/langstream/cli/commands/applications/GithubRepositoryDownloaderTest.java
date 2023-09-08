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

import java.net.URI;
import org.junit.jupiter.api.Test;

class GithubRepositoryDownloaderTest {

    @Test
    void testParseUrl() {
        final GithubRepositoryDownloader.RequestedDirectory request =
                GithubRepositoryDownloader.parseRequest(
                        URI.create(
                                "https://github.com/LangStream/langstream/tree/main/examples/applications/astradb-sink"));
        assertEquals("LangStream", request.getOwner());
        assertEquals("langstream", request.getRepository());
        assertEquals("main", request.getBranch());
        assertEquals("examples/applications/astradb-sink", request.getDirectory());
    }

    @Test
    void testParseUrlBlob() {
        final GithubRepositoryDownloader.RequestedDirectory request =
                GithubRepositoryDownloader.parseRequest(
                        URI.create(
                                "https://github.com/LangStream/langstream/blob/v0.0.13/examples/instances/astra.yaml"));
        assertEquals("LangStream", request.getOwner());
        assertEquals("langstream", request.getRepository());
        assertEquals("v0.0.13", request.getBranch());
        assertEquals("examples/instances/astra.yaml", request.getDirectory());
    }

    @Test
    void testParseUrlRoot() {
        final GithubRepositoryDownloader.RequestedDirectory request =
                GithubRepositoryDownloader.parseRequest(
                        URI.create("https://github.com/LangStream/langstream/tree/main"));
        assertEquals("LangStream", request.getOwner());
        assertEquals("langstream", request.getRepository());
        assertEquals("main", request.getBranch());
        assertNull(request.getDirectory());
    }
}
