package com.datastax.oss.sga.cli.commands;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import picocli.CommandLine;

@CommandLine.Command(name = "deploy",
        description = "Deploy a SGA application")
public class DeployCmd extends BaseCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(names = {"-app", "--application"}, description = "Application directory path", required = true)
    private String appPath;

    @CommandLine.Option(names = {"-i", "--instance"}, description = "Instance file path", required = true)
    private String instanceFilePath;

    @Override
    @SneakyThrows
    public void run() {
        final File appDirectory = checkFileExists(appPath);
        final File instanceFile = checkFileExists(instanceFilePath);

        final Path tempZip = Files.createTempFile("app", ".zip");
        debug("building local zip at " + tempZip.toAbsolutePath());
        try (final ZipFile zip = new ZipFile(tempZip.toFile());) {
            log("packaging app: %s".formatted(appDirectory.getAbsolutePath()));
            if (appDirectory.isDirectory()) {
                for (File file : appDirectory.listFiles()) {
                    zip.addFile(file);
                }
            } else {
                zip.addFile(appDirectory);
            }
            log("using instance: %s".formatted(instanceFile.getAbsolutePath()));
            final ZipParameters zipParameters = new ZipParameters();
            zipParameters.setFileNameInZip("instance.yaml");
            zip.addFile(instanceFile, zipParameters);
        }


            String boundary = new BigInteger(256, new Random()).toString();
            final HttpResponse<String> response = getHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("%s/api/applications/%s".formatted(getBaseWebServiceUrl(), name)))
                            .header("Content-Type", "multipart/form-data;boundary=" + boundary)
                            .PUT(multiPartBodyPublisher(tempZip, boundary))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                log("Application deployed");
            } else {
                log("Application not deployed, error: "+ response.statusCode());
                log(response.body());
            }

    }

    private File checkFileExists(String path) {
        final File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("File " + path + " does not exist");
        }
        return file;
    }

    public static HttpRequest.BodyPublisher multiPartBodyPublisher(Path path, String boundary) throws IOException {
        List<byte[]> byteArrays = new ArrayList<>();
        final String beforeFile = "--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\nContent-Type: %s\r\n\r\n"
                .formatted(boundary, path.getFileName(), Files.probeContentType(path));
        byteArrays.add(beforeFile.getBytes(StandardCharsets.UTF_8));
                byteArrays.add(Files.readAllBytes(path));
        byteArrays.add("\r\n--%s--\r\n".formatted(boundary).getBytes(StandardCharsets.UTF_8));
        return HttpRequest.BodyPublishers.ofByteArrays(byteArrays);
    }

}
