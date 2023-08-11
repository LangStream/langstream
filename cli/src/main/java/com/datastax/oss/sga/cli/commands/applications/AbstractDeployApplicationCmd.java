/**
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
package com.datastax.oss.sga.cli.commands.applications;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Dependency;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import picocli.CommandLine;


public abstract class AbstractDeployApplicationCmd extends BaseApplicationCmd {

    @CommandLine.Command(name = "deploy",
            description = "Deploy a SGA application")
    public static class DeployApplicationCmd extends AbstractDeployApplicationCmd {

        @CommandLine.Parameters(description = "Name of the application")
        private String name;

        @CommandLine.Option(names = {"-app",
                "--application"}, description = "Application directory path", required = true)
        private String appPath;

        @CommandLine.Option(names = {"-i",
                "--instance"}, description = "Instance file path", required = true)
        private String instanceFilePath;

        @CommandLine.Option(names = {"-s", "--secrets"}, description = "Secrets file path")
        private String secretFilePath;


        @Override
        String appName() {
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


    @CommandLine.Command(name = "update",
            description = "Update an existing SGA application")
    public static class UpdateApplicationCmd extends AbstractDeployApplicationCmd {

        @CommandLine.Parameters(description = "Name of the application")
        private String name;

        @CommandLine.Option(names = {"-app",
                "--application"}, description = "Application directory path")
        private String appPath;

        @CommandLine.Option(names = {"-i",
                "--instance"}, description = "Instance file path.")
        private String instanceFilePath;

        @CommandLine.Option(names = {"-s", "--secrets"}, description = "Secrets file path")
        private String secretFilePath;


        @Override
        String appName() {
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

    abstract String appName();

    abstract String appPath();

    abstract String instanceFilePath();

    abstract String secretFilePath();

    abstract boolean isUpdate();


    @Override
    @SneakyThrows
    public void run() {
        final File appDirectory = checkFileExists(appPath());
        final File instanceFile = checkFileExists(instanceFilePath());
        final File secretsFile = checkFileExists(secretFilePath());
        final String name = appName();

        if (isUpdate() && appDirectory == null && instanceFile == null && secretsFile == null) {
            throw new IllegalArgumentException("no application, instance or secrets file provided");
        }

        if (appDirectory != null) {
            // parse locally the application in order to validate it
            // and to get the dependencies
            final Application applicationInstance =
                    ModelBuilder.buildApplicationInstance(List.of(appDirectory.toPath()));
            downloadDependencies(applicationInstance, appDirectory.toPath());
        }

        final Path tempZip = buildZip(appDirectory, instanceFile, secretsFile, s -> log(s));

        long size = Files.size(tempZip);
        log("deploying application: %s (%d KB)".formatted(name, size / 1024));

        String boundary = new BigInteger(256, new Random()).toString();

        final String path = tenantAppPath("/" + name);
        final String contentType = "multipart/form-data;boundary=%s".formatted(boundary);
        final HttpRequest.BodyPublisher bodyPublisher = multiPartBodyPublisher(tempZip, boundary);
        if (isUpdate()) {
            final HttpRequest request = newPut(path, contentType, bodyPublisher);
            http(request);
            log("application %s updated".formatted(name));
        } else {
            final HttpRequest request = newPost(path, contentType, bodyPublisher);
            http(request);
            log("application %s deployed".formatted(name));
        }

    }

    private void downloadDependencies(Application applicationInstance, Path directory) throws Exception {
        if (applicationInstance.getDependencies() != null) {
            for (Dependency dependency : applicationInstance.getDependencies()) {
                URL url = new URL(dependency.url());

                String outputPath = switch (dependency.type()) {
                    case "java-library" -> "java/lib";
                    default -> throw new RuntimeException("unsupported dependency type: " + dependency.type());
                };
                Path output = directory.resolve(outputPath);
                if (!Files.exists(output)) {
                    Files.createDirectories(output);
                }
                String rawFileName = url.getFile().substring(url.getFile().lastIndexOf('/') + 1);
                Path fileName = output.resolve(rawFileName);

                if (Files.isRegularFile(fileName)) {

                    if (!checkChecksum(fileName, dependency.sha512sum())) {
                        log("File seems corrupted, deleting it");
                        Files.delete(fileName);
                    } else {
                        log("Dependency: %s at %s".formatted(fileName, fileName.toAbsolutePath()));
                        continue;
                    }
                }

                log("downloading dependency: %s to %s".formatted(fileName, fileName.toAbsolutePath()));
                final HttpRequest request = newDependencyGet(url);
                http(request, HttpResponse.BodyHandlers.ofFile(fileName));

                if (!checkChecksum(fileName, dependency.sha512sum())) {
                    log("File still seems corrupted. Please double check the checksum and try again.");
                    Files.delete(fileName);
                    throw new IOException("File at " + url + ", seems corrupted");
                }

                log("dependency downloaded");
            }
        }
    }

    private boolean checkChecksum(Path fileName, String sha512sum) throws Exception {
        MessageDigest instance = MessageDigest.getInstance("SHA-512");
        try (DigestInputStream inputStream = new DigestInputStream(
                new BufferedInputStream(Files.newInputStream(fileName)), instance);) {
            while (inputStream.read() != -1) {
            }
        }
        byte[] digest = instance.digest();
        String base16encoded = bytesToHex(digest);
        if (!sha512sum.equals(base16encoded)) {
            log("Computed checksum: %s".formatted(base16encoded));
            log("Expected checksum: %s".formatted(sha512sum));
        }
        return sha512sum.equals(base16encoded);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static Path buildZip(File appDirectory, File instanceFile, File secretsFile,
                                Consumer<String> logger) throws IOException {
        final Path tempZip = Files.createTempFile("app", ".zip");
        try (final ZipFile zip = new ZipFile(tempZip.toFile());) {
            addApp(appDirectory, zip, logger);
            addInstance(instanceFile, zip, logger);
            addSecrets(secretsFile, zip, logger);
        }
        return tempZip;
    }

    private static void addApp(File appDirectory, ZipFile zip, Consumer<String> logger) throws ZipException {
        if (appDirectory == null) {
            return;
        }
        logger.accept("packaging app: %s".formatted(appDirectory.getAbsolutePath()));
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

    private static void addInstance(File instanceFile, ZipFile zip, Consumer<String> logger) throws ZipException {
        if (instanceFile == null) {
            return;
        }
        logger.accept("using instance: %s".formatted(instanceFile.getAbsolutePath()));
        final ZipParameters zipParameters = new ZipParameters();
        zipParameters.setFileNameInZip("instance.yaml");
        zip.addFile(instanceFile, zipParameters);
    }

    private static void addSecrets(File secretsFile, ZipFile zip, Consumer<String> logger) throws ZipException {
        if (secretsFile == null) {
            return;
        }
        logger.accept("using secrets: %s".formatted(secretsFile.getAbsolutePath()));
        final ZipParameters zipParameters = new ZipParameters();
        zipParameters.setFileNameInZip("secrets.yaml");
        zip.addFile(secretsFile, zipParameters);
    }

    private File checkFileExists(String path) {
        if (path == null) {
            return null;
        }
        final File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("File " + path + " does not exist");
        }
        return file;
    }

    public static byte[] buildMultipartContentForAppZip(Path path, String boundary) throws IOException {
        List<byte[]> byteArrays = new ArrayList<>();
        final String beforeFile =
                "--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\nContent-Type: %s\r\n\r\n"
                        .formatted(boundary, path.getFileName(), Files.probeContentType(path));
        byteArrays.add(beforeFile.getBytes(StandardCharsets.UTF_8));
        byteArrays.add(Files.readAllBytes(path));
        byteArrays.add("\r\n--%s--\r\n".formatted(boundary).getBytes(StandardCharsets.UTF_8));
        return concatBytes(byteArrays.toArray(new byte[0][]));
    }

    @SneakyThrows
    private static byte[] concatBytes(byte[]... arrays) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            outputStream.write(array);
        }
        return outputStream.toByteArray();
    }

    private static HttpRequest.BodyPublisher multiPartBodyPublisher(Path path, String boundary) throws IOException {
        return HttpRequest.BodyPublishers.ofByteArray(buildMultipartContentForAppZip(path, boundary));
    }

}
