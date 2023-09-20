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

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import lombok.Data;
import lombok.SneakyThrows;
import org.eclipse.jgit.api.Git;

public class GithubRepositoryDownloader {

    @Data
    static class RequestedDirectory {
        private String owner;
        private String repository;
        private String branch;
        private String directory;
    }

    @SneakyThrows
    public static File downloadGithubRepository(URI uri, Consumer<String> logger) {
        RequestedDirectory requestedDirectory = parseRequest(uri);

        final Path directory = Files.createTempDirectory("langstream");
        logger.accept(
                String.format("Cloning GitHub repository %s locally", uri));

        final long start = System.currentTimeMillis();
        Git.cloneRepository()
                .setURI(
                        String.format(
                                "https://github.com/%s/%s.git",
                                requestedDirectory.getOwner(), requestedDirectory.getRepository()))
                .setDirectory(directory.toFile())
                .setBranch(requestedDirectory.getBranch())
                .setDepth(1)
                .call();
        final long time = (System.currentTimeMillis() - start) / 1000;
        logger.accept(String.format("downloaded GitHub directory (%d s)", time));
        final Path result = directory.resolve(requestedDirectory.getDirectory());
        return result.toFile();
    }

    static RequestedDirectory parseRequest(URI uri) {
        final String path = uri.getPath();
        final String[] paths = path.split("/", 6);
        if (paths.length < 5) {
            throw new IllegalArgumentException(
                    "Invalid github url. Expected format: https://github"
                            + ".com/<owner>/<repository>/tree|blob/<branch>[/directory]");
        }
        RequestedDirectory requestedDirectory = new RequestedDirectory();
        requestedDirectory.setOwner(paths[1]);
        requestedDirectory.setRepository(paths[2]);
        requestedDirectory.setBranch(paths[4]);
        if (paths.length > 5) {
            requestedDirectory.setDirectory(paths[5]);
        }
        return requestedDirectory;
    }
}
