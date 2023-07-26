package com.datastax.oss.sga.tests;

import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class PythonFunctionIT extends BaseEndToEndTest {

    @Test
    public void test() throws Exception {
        executeCliCommand("configure", "webServiceUrl", controlPlaneBaseUrl);
        String testAppsBaseDir = "src/test/resources/apps";
        String testInstanceBaseDir = "src/test/resources/instances";
        String testSecretBaseDir = "src/test/resources/secrets";
        executeCliCommand("apps", "deploy", "my-test-app",
                "-app", Paths.get(testAppsBaseDir, "python-function").toFile().getAbsolutePath(),
                "-i", Paths.get(testInstanceBaseDir, "kafka-kubernetes.yaml").toFile().getAbsolutePath(),
                "-s", Paths.get(testSecretBaseDir, "secret1.yaml").toFile().getAbsolutePath());

        produceKafkaMessages("input-topic", 1);
        receiveKafkaMessages("output-topic", 1);
    }

}

