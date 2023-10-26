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

import ai.langstream.cli.CLILogger;
import ai.langstream.cli.util.GitClient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
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

    @Test
    void testDownload() throws Exception {

        AtomicBoolean injectUpdateFailure = new AtomicBoolean(false);

        final Path home = Files.createTempDirectory("test");

        final GitClient client =
                new GitClient() {
                    @Override
                    public void cloneRepository(Path directory, String uri, String branch)
                            throws IOException {
                        Files.createDirectories(directory.resolve("examples"));
                        Files.writeString(
                                directory.resolve("examples").resolve("my-file"),
                                "content! " + branch);
                    }

                    @Override
                    public String updateRepository(Path directory, String branch)
                            throws IOException {
                        if (injectUpdateFailure.get()) {
                            throw new IOException("inject failure");
                        }
                        Files.writeString(
                                directory.resolve("examples").resolve("my-file"),
                                "content updated! " + branch);
                        return "xxx";
                    }
                };
        final GithubRepositoryDownloader downloader =
                new GithubRepositoryDownloader(client, new CLILogger.SystemCliLogger(), home);

        File file =
                downloader.downloadGithubRepository(
                        URI.create("https://localhost/LangStream/langstream/tree/main/examples"),
                        false);

        assertEquals("content! main", Files.readString(file.toPath().resolve("my-file")));
        assertFalse(home.resolve("ghrepos").toFile().exists());

        file =
                downloader.downloadGithubRepository(
                        URI.create("https://localhost/LangStream/langstream2/tree/main/examples"),
                        true);
        assertEquals("content! main", Files.readString(file.toPath().resolve("my-file")));
        assertTrue(home.resolve("ghrepos").toFile().exists());
        assertEquals(
                home.resolve("ghrepos")
                        .resolve("LangStream")
                        .resolve("langstream2")
                        .resolve("main")
                        .resolve("examples")
                        .toFile()
                        .getAbsolutePath(),
                file.getAbsolutePath());

        file =
                downloader.downloadGithubRepository(
                        URI.create("https://localhost/LangStream/langstream2/tree/main/examples"),
                        true);
        assertEquals("content! main", Files.readString(file.toPath().resolve("my-file")));
        assertTrue(home.resolve("ghrepos").toFile().exists());
        assertEquals(
                home.resolve("ghrepos")
                        .resolve("LangStream")
                        .resolve("langstream2")
                        .resolve("main")
                        .resolve("examples")
                        .toFile()
                        .getAbsolutePath(),
                file.getAbsolutePath());

        file =
                new GithubRepositoryDownloader(client, new CLILogger.SystemCliLogger(), home)
                        .downloadGithubRepository(
                                URI.create(
                                        "https://localhost/LangStream/langstream2/tree/main/examples"),
                                true);
        assertEquals("content updated! main", Files.readString(file.toPath().resolve("my-file")));
        assertTrue(home.resolve("ghrepos").toFile().exists());
        assertEquals(
                home.resolve("ghrepos")
                        .resolve("LangStream")
                        .resolve("langstream2")
                        .resolve("main")
                        .resolve("examples")
                        .toFile()
                        .getAbsolutePath(),
                file.getAbsolutePath());

        injectUpdateFailure.set(true);

        file =
                new GithubRepositoryDownloader(client, new CLILogger.SystemCliLogger(), home)
                        .downloadGithubRepository(
                                URI.create(
                                        "https://localhost/LangStream/langstream2/tree/main/examples"),
                                true);
        assertEquals("content! main", Files.readString(file.toPath().resolve("my-file")));
        assertFalse(home.resolve("ghrepos").toFile().exists());
    }
}
