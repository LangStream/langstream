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

import ai.langstream.cli.CLILogger;
import ai.langstream.cli.util.GitClient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;

@AllArgsConstructor
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

    private final GitClient client;
    private final CLILogger logger;
    private final Path cliHomeDirectory;

    /**
     * This cache avoids cloning/updating the same repository multiple times in the same process.
     * For example passing secrets and applications from the same repository in the same command.
     */
    private final Map<RepoRef, Path> clonedUpdatedRepos = new HashMap<>();

    @SneakyThrows
    public File downloadGithubRepository(URI uri, boolean useLocalGithubRepos) {
        final RequestedDirectory requestedDirectory = parseRequest(uri);
        Path cloneToDirectory;

        final RepoRef repoRef = new RepoRef(requestedDirectory);
        final boolean upToDate = clonedUpdatedRepos.containsKey(repoRef);
        if (!upToDate || !useLocalGithubRepos) {
            final long start = System.currentTimeMillis();
            if (useLocalGithubRepos && cliHomeDirectory != null) {
                try {
                    cloneToDirectory =
                            cloneOrUpdateFromGithubReposCache(uri, requestedDirectory, start);
                } catch (IOException ioException) {
                    logger.log(
                            String.format(
                                    "Failed to update local GitHub repository %s, falling back to cloning to a temporary directory",
                                    uri));
                    final Path githubReposPath = resolveGithubReposPath(cliHomeDirectory);
                    try {
                        deleteDirectory(githubReposPath);
                    } catch (IOException e) {
                        logger.log(
                                String.format(
                                        "Failed to delete local GitHub repository cache, please remove manually at path: %s, error: %s",
                                        githubReposPath, e.getMessage()));
                    }
                    cloneToDirectory = Files.createTempDirectory("langstream");
                    cloneRepository(uri, requestedDirectory, cloneToDirectory);
                }
            } else {
                cloneToDirectory = Files.createTempDirectory("langstream");
                cloneRepository(uri, requestedDirectory, cloneToDirectory);
            }
            clonedUpdatedRepos.put(repoRef, cloneToDirectory);
        } else {
            cloneToDirectory = clonedUpdatedRepos.get(repoRef);
            logger.log(String.format("Using cached GitHub repository %s", cloneToDirectory));
        }
        final Path result = cloneToDirectory.resolve(requestedDirectory.getDirectory());
        return result.toFile();
    }

    private void deleteDirectory(Path githubReposPath) throws IOException {
        if (githubReposPath.toFile().isDirectory()) {
            final File[] listFiles = githubReposPath.toFile().listFiles();
            for (File listFile : listFiles) {
                deleteDirectory(listFile.toPath());
            }
        }
        Files.deleteIfExists(githubReposPath);
    }

    private static Path resolveGithubReposPath(Path langstreamCLIHomeDirectory) {
        return langstreamCLIHomeDirectory.resolve("ghrepos");
    }

    private Path cloneOrUpdateFromGithubReposCache(
            URI uri, RequestedDirectory requestedDirectory, long start) throws IOException {
        Path cloneToDirectory;
        final Path repos = resolveGithubReposPath(cliHomeDirectory);
        cloneToDirectory =
                repos.resolve(
                        Path.of(
                                requestedDirectory.getOwner(),
                                requestedDirectory.getRepository(),
                                requestedDirectory.getBranch()));
        if (cloneToDirectory.toFile().exists()) {
            logger.log(String.format("Updating local GitHub repository %s", cloneToDirectory));
            final String sha =
                    client.updateRepository(cloneToDirectory, requestedDirectory.getBranch());
            final long time = (System.currentTimeMillis() - start) / 1000;
            logger.log(String.format("Updated local GitHub repository to %s (%d s)", sha, time));
        } else {
            Files.createDirectories(cloneToDirectory);
            cloneRepository(uri, requestedDirectory, cloneToDirectory);
        }
        return cloneToDirectory;
    }

    private void cloneRepository(
            URI uri, RequestedDirectory requestedDirectory, Path cloneToDirectory)
            throws IOException {
        final long start = System.currentTimeMillis();
        logger.log(String.format("Cloning GitHub repository %s locally", uri));
        final String githubUri =
                String.format(
                        "https://github.com/%s/%s.git",
                        requestedDirectory.getOwner(), requestedDirectory.getRepository());
        client.cloneRepository(cloneToDirectory, githubUri, requestedDirectory.getBranch());
        final long time = (System.currentTimeMillis() - start) / 1000;
        logger.log(String.format("Downloaded GitHub repository (%d s)", time));
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
