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
import ai.langstream.cli.util.LocalFileReferenceResolver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
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
    }

    abstract String applicationId();

    abstract String appPath();

    abstract String instanceFilePath();

    abstract String secretFilePath();

    abstract boolean isUpdate();

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
            downloadDependencies(appDirectory.toPath());
        }

        final Path tempZip = buildZip(appDirectory, this::log);

        long size = Files.size(tempZip);
        log(String.format("deploying application: %s (%d KB)", applicationId, size / 1024));

        final Map<String, Object> contents = new HashMap<>();
        contents.put("app", tempZip);
        if (instanceFile != null) {
            contents.put(
                    "instance",
                    LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                            instanceFile.toPath()));
        }
        if (secretsFile != null) {
            contents.put(
                    "secrets",
                    LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                            secretsFile.toPath()));
        }
        final MultiPartBodyPublisher bodyPublisher = buildMultipartContentForAppZip(contents);

        if (isUpdate()) {
            getClient().applications().update(applicationId, bodyPublisher);
            log(String.format("application %s updated", applicationId));
        } else {
            getClient().applications().deploy(applicationId, bodyPublisher);
            log(String.format("application %s deployed", applicationId));
        }
    }

    @AllArgsConstructor
    @Getter
    private static class Dependency {
        private String name;
        private String url;
        private String type;
        private String sha512sum;
    }

    private void downloadDependencies(Path directory) throws Exception {

        final Path configuration = directory.resolve("configuration.yaml");
        if (!Files.exists(configuration)) {
            return;
        }
        final Map<String, Object> map =
                yamlConfigReader.readValue(configuration.toFile(), Map.class);
        final List<Map<String, Object>> dependencies =
                (List<Map<String, Object>>) map.get("dependencies");
        if (dependencies == null) {
            return;
        }
        final List<Dependency> dependencyList =
                dependencies.stream()
                        .map(
                                dependency ->
                                        new Dependency(
                                                (String) dependency.get("name"),
                                                (String) dependency.get("url"),
                                                (String) dependency.get("type"),
                                                (String) dependency.get("sha512sum")))
                        .collect(Collectors.toList());

        for (Dependency dependency : dependencyList) {
            URL url = new URL(dependency.getUrl());

            final String outputPath;
            switch (dependency.getType()) {
                case "java-library":
                    outputPath = "java/lib";
                    break;
                default:
                    throw new RuntimeException(
                            "unsupported dependency type: " + dependency.getType());
            }
            ;
            Path output = directory.resolve(outputPath);
            if (!Files.exists(output)) {
                Files.createDirectories(output);
            }
            String rawFileName = url.getFile().substring(url.getFile().lastIndexOf('/') + 1);
            Path fileName = output.resolve(rawFileName);

            if (Files.isRegularFile(fileName)) {

                if (!checkChecksum(fileName, dependency.getSha512sum())) {
                    log("File seems corrupted, deleting it");
                    Files.delete(fileName);
                } else {
                    log(String.format("Dependency: %s at %s", fileName, fileName.toAbsolutePath()));
                    continue;
                }
            }

            log(
                    String.format(
                            "downloading dependency: %s to %s",
                            fileName, fileName.toAbsolutePath()));
            final HttpRequest request = getClient().newDependencyGet(url);
            getClient().http(request, HttpResponse.BodyHandlers.ofFile(fileName));

            if (!checkChecksum(fileName, dependency.getSha512sum())) {
                log("File still seems corrupted. Please double check the checksum and try again.");
                Files.delete(fileName);
                throw new IOException("File at " + url + ", seems corrupted");
            }

            log("dependency downloaded");
        }
    }

    private boolean checkChecksum(Path fileName, String sha512sum) throws Exception {
        MessageDigest instance = MessageDigest.getInstance("SHA-512");
        try (DigestInputStream inputStream =
                new DigestInputStream(
                        new BufferedInputStream(Files.newInputStream(fileName)), instance)) {
            while (inputStream.read() != -1) {}
        }
        byte[] digest = instance.digest();
        String base16encoded = bytesToHex(digest);
        if (!sha512sum.equals(base16encoded)) {
            log(String.format("Computed checksum: %s", base16encoded));
            log(String.format("Expected checksum: %s", sha512sum));
        }
        return sha512sum.equals(base16encoded);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static Path buildZip(File appDirectory, Consumer<String> logger) throws IOException {
        final Path tempZip = Files.createTempFile("app", ".zip");
        try (final ZipFile zip = new ZipFile(tempZip.toFile())) {
            addApp(appDirectory, zip, logger);
        }
        return tempZip;
    }

    private static void addApp(File appDirectory, ZipFile zip, Consumer<String> logger)
            throws ZipException {
        if (appDirectory == null) {
            return;
        }
        logger.accept(String.format("packaging app: %s", appDirectory.getAbsolutePath()));
        if (appDirectory.isDirectory()) {
            for (File file : appDirectory.listFiles()) {
                if (file.isDirectory()) {
                    zip.addFolder(file);
                } else {
                    zip.addFile(file);
                }
            }
        } else {
            zip.addFile(appDirectory);
        }
        logger.accept("app packaged");
    }

    @SneakyThrows
    private File checkFileExistsOrDownload(String path) {
        if (path == null) {
            return null;
        }
        if (path.startsWith("http://") || path.startsWith("https://")) {
            final HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(path))
                            .version(HttpClient.Version.HTTP_1_1)
                            .GET()
                            .build();
            final Path tempFile = Files.createTempFile("langstram", ".bin");
            final long start = System.currentTimeMillis();
            // use HttpResponse.BodyHandlers.ofByteArray() to get cleaner error messages
            final HttpResponse<byte[]> response =
                    getClient().http(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() >= 400) {
                throw new RuntimeException(
                        "Failed to download file: "
                                + path
                                + "\nReceived status code: "
                                + response.statusCode()
                                + "\n"
                                + response.body());
            }
            Files.write(tempFile, response.body());
            final long time = (System.currentTimeMillis() - start) / 1000;
            log(String.format("downloaded remote file %s (%d s)", path, time));
            return tempFile.toFile();
        }
        if (path.startsWith("file://")) {
            path = path.substring("file://".length());
        }
        final File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("File " + path + " does not exist");
        }
        return file;
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
