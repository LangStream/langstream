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

import ai.langstream.admin.client.util.MultiPartBodyPublisher;
import ai.langstream.cli.commands.GitIgnoreParser;
import ai.langstream.cli.util.LocalFileReferenceResolver;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import picocli.CommandLine;

public abstract class AbstractDeployApplicationCmd extends BaseApplicationCmd {

    @CommandLine.Command(name = "deploy", header = "Deploy a LangStream application")
    public static class DeployApplicationCmd extends AbstractDeployApplicationCmd {

        @CommandLine.Parameters(description = "Name of the application")
        private String name;

        @CommandLine.Option(
                names = {"-app", "--application"},
                description = "Application directory path",
                required = true)
        private String appPath;

        @CommandLine.Option(
                names = {"-i", "--instance"},
                description = "Instance file path",
                required = true)
        private String instanceFilePath;

        @CommandLine.Option(
                names = {"-s", "--secrets"},
                description = "Secrets file path")
        private String secretFilePath;

        @CommandLine.Option(
                names = {"--dry-run"},
                description = "")
        private boolean dryRun;

        @CommandLine.Option(
                names = {"-o"},
                description = "Output format for dry-run mode.")
        private Formats format = Formats.raw;

        @Override
        String applicationId() {
            return name;
        }

        @Override
        String appPath() {
            return appPath;
        }

        @Override
        String instanceFilePath() {
            return instanceFilePath;
        }

        @Override
        String secretFilePath() {
            return secretFilePath;
        }

        @Override
        boolean isUpdate() {
            return false;
        }

        @Override
        boolean isDryRun() {
            return dryRun;
        }

        @Override
        Formats format() {
            return format;
        }
    }

    @CommandLine.Command(name = "update", header = "Update an existing LangStream application")
    public static class UpdateApplicationCmd extends AbstractDeployApplicationCmd {

        @CommandLine.Parameters(description = "Name of the application")
        private String name;

        @CommandLine.Option(
                names = {"-app", "--application"},
                description = "Application directory path")
        private String appPath;

        @CommandLine.Option(
                names = {"-i", "--instance"},
                description = "Instance file path.")
        private String instanceFilePath;

        @CommandLine.Option(
                names = {"-s", "--secrets"},
                description = "Secrets file path")
        private String secretFilePath;

        @Override
        String applicationId() {
            return name;
        }

        @Override
        String appPath() {
            return appPath;
        }

        @Override
        String instanceFilePath() {
            return instanceFilePath;
        }

        @Override
        String secretFilePath() {
            return secretFilePath;
        }

        @Override
        boolean isUpdate() {
            return true;
        }

        @Override
        boolean isDryRun() {
            return false;
        }

        @Override
        Formats format() {
            return null;
        }
    }

    abstract String applicationId();

    abstract String appPath();

    abstract String instanceFilePath();

    abstract String secretFilePath();

    abstract boolean isUpdate();

    abstract boolean isDryRun();

    abstract Formats format();

    @Override
    @SneakyThrows
    public void run() {
        final File appDirectory = checkFileExistsOrDownload(appPath());
        final File instanceFile = checkFileExistsOrDownload(instanceFilePath());
        final File secretsFile = checkFileExistsOrDownload(secretFilePath());
        final String applicationId = applicationId();

        if (isUpdate() && appDirectory == null && instanceFile == null && secretsFile == null) {
            throw new IllegalArgumentException("no application, instance or secrets file provided");
        }
        if (!isUpdate() && (appDirectory == null || instanceFile == null)) {
            throw new IllegalArgumentException("application and instance files are required");
        }

        if (appDirectory != null) {
            downloadDependencies(appDirectory.toPath(), getClient(), this::log);
        }

        final Path tempZip = buildZip(appDirectory, this::log);

        long size = Files.size(tempZip);

        final Map<String, Object> contents = new HashMap<>();
        contents.put("app", tempZip);
        if (instanceFile != null) {
            try {
                contents.put(
                        "instance",
                        LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                                instanceFile.toPath()));
            } catch (Exception e) {
                log(
                        "Failed to resolve instance file references. Please double check the file path: "
                                + instanceFile.toPath());
                e.printStackTrace();
                throw e;
            }
        }
        if (secretsFile != null) {
            try {
                contents.put(
                        "secrets",
                        LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                                secretsFile.toPath()));
            } catch (Exception e) {
                log(
                        "Failed to resolve secrets file references. Please double check the file path: "
                                + secretsFile.toPath());
                throw e;
            }
        }

        final MultiPartBodyPublisher bodyPublisher = buildMultipartContentForAppZip(contents);

        if (isUpdate()) {
            log(String.format("updating application: %s (%d KB)", applicationId, size / 1024));
            getClient().applications().update(applicationId, bodyPublisher);
            log(String.format("application %s updated", applicationId));
        } else {
            final boolean dryRun = isDryRun();
            if (dryRun) {
                log(
                        String.format(
                                "resolving application: %s. Dry run mode is enabled, the application will NOT be deployed",
                                applicationId));
            } else {
                log(String.format("deploying application: %s (%d KB)", applicationId, size / 1024));
            }
            final String response =
                    getClient().applications().deploy(applicationId, bodyPublisher, dryRun);
            if (dryRun) {
                final Formats format = format();
                print(format == Formats.raw ? Formats.yaml : format, response, null, null);
            } else {
                log(String.format("application %s deployed", applicationId));
            }
        }
    }

    public static Path buildZip(File appDirectory, Consumer<String> logger) throws IOException {
        final Path tempZip = Files.createTempFile("app", ".zip");
        try (final ZipFile zip = new ZipFile(tempZip.toFile())) {
            addApp(appDirectory, zip, logger);
        }
        return tempZip;
    }

    private static void addApp(File appDirectory, ZipFile zip, Consumer<String> logger)
            throws IOException {
        if (appDirectory == null) {
            return;
        }
        logger.accept(String.format("packaging app: %s", appDirectory.getAbsolutePath()));
        if (appDirectory.isDirectory()) {
            File ignoreFile = appDirectory.toPath().resolve(".langstreamignore").toFile();
            if (ignoreFile.exists()) {
                GitIgnoreParser parser = new GitIgnoreParser(ignoreFile.toPath());
                addDirectoryFilesWithLangstreamIgnore(appDirectory, appDirectory, parser, zip);
            } else {
                for (File file : appDirectory.listFiles()) {
                    if (file.isDirectory()) {
                        zip.addFolder(file);
                    } else {
                        zip.addFile(file);
                    }
                }
            }
        } else {
            zip.addFile(appDirectory);
        }
        logger.accept("app packaged");
    }

    private static void addDirectoryFilesWithLangstreamIgnore(
            File appDirectory, File directory, GitIgnoreParser parser, ZipFile zip)
            throws IOException {
        for (File file : directory.listFiles()) {
            if (!parser.matches(file)) {
                if (file.isDirectory()) {
                    addDirectoryFilesWithLangstreamIgnore(appDirectory, file, parser, zip);
                } else {
                    ZipParameters zipParameters = new ZipParameters();
                    String filename = appDirectory.toURI().relativize(file.toURI()).getPath();
                    zipParameters.setFileNameInZip(filename);
                    zip.addFile(file, zipParameters);
                }
            }
        }
    }

    public static MultiPartBodyPublisher buildMultipartContentForAppZip(
            Map<String, Object> formData) {
        final MultiPartBodyPublisher multiPartBodyPublisher = new MultiPartBodyPublisher();
        for (Map.Entry<String, Object> formDataEntry : formData.entrySet()) {
            if (formDataEntry.getValue() instanceof Path) {
                multiPartBodyPublisher.addPart(
                        formDataEntry.getKey(), (Path) formDataEntry.getValue());
            } else {
                multiPartBodyPublisher.addPart(
                        formDataEntry.getKey(), formDataEntry.getValue().toString());
            }
        }
        return multiPartBodyPublisher;
    }
}
