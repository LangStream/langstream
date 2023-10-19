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

import ai.langstream.cli.LangStreamCLI;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import lombok.Data;
import lombok.SneakyThrows;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;

public class GithubRepositoryDownloader {

    @Data
    static class RequestedDirectory {
        private String owner;
        private String repository;
        private String branch;
        private String directory;
    }

    @Data
    static class RepoRef {
        private String owner;
        private String repository;
        private String branch;

        public RepoRef(RequestedDirectory requestedDirectory) {
            this.owner = requestedDirectory.getOwner();
            this.repository = requestedDirectory.getRepository();
            this.branch = requestedDirectory.getBranch();
        }

    }

    /**
     * This cache avoids cloning/updating the same repository multiple times in the same process.
     * For example passing secrets and applications from the same repository in the same command.
     */
    static Map<RepoRef, Path> clonedUpdatedRepos = new HashMap<>();

    @SneakyThrows
    public static File downloadGithubRepository(URI uri, Consumer<String> logger) {
        final RequestedDirectory requestedDirectory = parseRequest(uri);
        Path cloneToDirectory;

        final RepoRef repoRef = new RepoRef(requestedDirectory);
        final boolean upToDate = clonedUpdatedRepos.containsKey(repoRef);
        if (!upToDate) {
            final long start = System.currentTimeMillis();
            final Path langstreamCLIHomeDirectory = LangStreamCLI.getLangstreamCLIHomeDirectory();
            if (langstreamCLIHomeDirectory != null) {
                cloneToDirectory = cloneOrUpdateFromGithubReposCache(uri, logger, requestedDirectory, start, langstreamCLIHomeDirectory);
            } else {
                cloneToDirectory = Files.createTempDirectory("langstream");
                cloneRepository(uri, logger, requestedDirectory, cloneToDirectory);
            }
            clonedUpdatedRepos.put(repoRef, cloneToDirectory);
        } else {
            logger.accept(String.format("Using cached GitHub repository %s", uri));
            cloneToDirectory = clonedUpdatedRepos.get(repoRef);
        }
        final Path result = cloneToDirectory.resolve(requestedDirectory.getDirectory());
        return result.toFile();
    }

    private static Path cloneOrUpdateFromGithubReposCache(URI uri, Consumer<String> logger, RequestedDirectory requestedDirectory, long start,
                                Path langstreamCLIHomeDirectory) throws GitAPIException, IOException {
        Path cloneToDirectory;
        final Path repos = langstreamCLIHomeDirectory.resolve("ghrepos");
        cloneToDirectory = repos.resolve(
                Path.of(requestedDirectory.getOwner(), requestedDirectory.getRepository(),
                        requestedDirectory.getBranch()));
        if (cloneToDirectory.toFile().exists()) {
            logger.accept(String.format("Updating local GitHub repository %s", uri));
            try (final Git open = Git.open(cloneToDirectory.toFile());) {
                open.fetch().setRefSpecs(new RefSpec("refs/heads/" + requestedDirectory.getBranch())).call();
                open.reset().setMode(ResetCommand.ResetType.HARD)
                        .setRef("origin/" + requestedDirectory.getBranch()).call();
                final RevCommit revCommit = open.log().setMaxCount(1).call().iterator().next();
                final String sha = revCommit.getId().abbreviate(8).name();
                final long time = (System.currentTimeMillis() - start) / 1000;
                logger.accept(String.format("Updated local GitHub repository to %s (%d s)", sha, time));
            }
        } else {
            Files.createDirectories(cloneToDirectory);
            cloneRepository(uri, logger, requestedDirectory, cloneToDirectory);
        }
        return cloneToDirectory;
    }

    private static void cloneRepository(URI uri, Consumer<String> logger, RequestedDirectory requestedDirectory,
                                  Path cloneToDirectory) throws GitAPIException {
        final long start = System.currentTimeMillis();
        logger.accept(String.format("Cloning GitHub repository %s locally", uri));
        Git.cloneRepository()
                .setURI(
                        String.format(
                                "https://github.com/%s/%s.git",
                                requestedDirectory.getOwner(), requestedDirectory.getRepository()))
                .setDirectory(cloneToDirectory.toFile())
                .setBranch(requestedDirectory.getBranch())
                .setDepth(1)
                .call();
        final long time = (System.currentTimeMillis() - start) / 1000;
        logger.accept(String.format("Downloaded GitHub repository (%d s)", time));
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
