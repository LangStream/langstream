package com.datastax.oss.sga.cli.commands.applications;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import picocli.CommandLine;

@CommandLine.Command(name = "deploy",
        description = "Deploy a SGA application")
public class DeployApplicationCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(names = {"-app", "--application"}, description = "Application directory path", required = true)
    private String appPath;

    @CommandLine.Option(names = {"-i", "--instance"}, description = "Instance file path", required = true)
    private String instanceFilePath;

    @CommandLine.Option(names = {"-s", "--secrets"}, description = "Secrets file path")
    private String secretFilePath;

    @Override
    @SneakyThrows
    public void run() {
        final File appDirectory = checkFileExists(appPath);
        final File instanceFile = checkFileExists(instanceFilePath);
        final File secretsFile = checkFileExists(secretFilePath);

        final Path tempZip = Files.createTempFile("app", ".zip");
        debug("building local zip at " + tempZip.toAbsolutePath());
        try (final ZipFile zip = new ZipFile(tempZip.toFile());) {
            addApp(appDirectory, zip);
            addInstance(instanceFile, zip);
            addSecrets(secretsFile, zip);
        }


        log("deploying application: %s".formatted(name));
        String boundary = new BigInteger(256, new Random()).toString();
        http(newPut("/applications/%s".formatted(name),
                "multipart/form-data;boundary=%s".formatted(boundary),
                multiPartBodyPublisher(tempZip, boundary)));
        log("application %s deployed".formatted(name));

    }

    private void addApp(File appDirectory, ZipFile zip) throws ZipException {
        log("packaging app: %s".formatted(appDirectory.getAbsolutePath()));
        if (appDirectory.isDirectory()) {
            for (File file : appDirectory.listFiles()) {
                zip.addFile(file);
            }
        } else {
            zip.addFile(appDirectory);
        }
        log("app packaged");
    }

    private void addInstance(File instanceFile, ZipFile zip) throws ZipException {
        log("using instance: %s".formatted(instanceFile.getAbsolutePath()));
        final ZipParameters zipParameters = new ZipParameters();
        zipParameters.setFileNameInZip("instance.yaml");
        zip.addFile(instanceFile, zipParameters);
    }

    private void addSecrets(File secretsFile, ZipFile zip) throws ZipException {
        if (secretsFile == null) {
            return;
        }
        log("using secrets: %s".formatted(secretsFile.getAbsolutePath()));
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

    public static HttpRequest.BodyPublisher multiPartBodyPublisher(Path path, String boundary) throws IOException {
        List<byte[]> byteArrays = new ArrayList<>();
        final String beforeFile =
                "--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\nContent-Type: %s\r\n\r\n"
                        .formatted(boundary, path.getFileName(), Files.probeContentType(path));
        byteArrays.add(beforeFile.getBytes(StandardCharsets.UTF_8));
        byteArrays.add(Files.readAllBytes(path));
        byteArrays.add("\r\n--%s--\r\n".formatted(boundary).getBytes(StandardCharsets.UTF_8));
        return HttpRequest.BodyPublishers.ofByteArrays(byteArrays);
    }

}
