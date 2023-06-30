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
import picocli.CommandLine;

@CommandLine.Command(name = "deploy",
        description = "Deploy a SGA application")
public class DeployCmd extends BaseCmd {

    @CommandLine.Option(names = {"-n", "--name"}, description = "Name")
    private String name;

    @CommandLine.Option(names = {"-p", "--package"}, description = "Package path")
    private String packagePath;

    @Override
    @SneakyThrows
    public void run() {
        final File file = new File(packagePath);
        if (!file.exists()) {
            throw new RuntimeException("File " + packagePath + " does not exist");
        }
        if (!file.isDirectory()) {
            throw new RuntimeException("Path " + packagePath + " is not a directory");
        }

        final Path tempZip = Files.createTempFile("app", ".zip");


        try (final ZipFile zip = new ZipFile(tempZip.toFile());) {
            log("packaging folder: %s".formatted(file.getAbsolutePath()));
            zip.addFolder(file);
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
