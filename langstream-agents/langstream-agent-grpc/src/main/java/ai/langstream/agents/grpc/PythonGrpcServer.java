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
package ai.langstream.agents.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PythonGrpcServer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path codeDirectory;
    private final Map<String, Object> configuration;
    private Process pythonProcess;

    public PythonGrpcServer(Path codeDirectory, Map<String, Object> configuration) {
        this.codeDirectory = codeDirectory;
        this.configuration = configuration;
    }

    public ManagedChannel start() throws Exception {
        // Get a free port
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        }

        Path pythonCodeDirectory = codeDirectory.resolve("python");
        log.info("Python code directory {}", pythonCodeDirectory);

        final String pythonPath = System.getenv("PYTHONPATH");
        final String newPythonPath =
                "%s:%s:%s"
                        .formatted(
                                pythonPath,
                                pythonCodeDirectory.toAbsolutePath(),
                                pythonCodeDirectory.resolve("lib").toAbsolutePath());

        // copy input/output to standard input/output of the java process
        // this allows to use "kubectl logs" easily
        ProcessBuilder processBuilder =
                new ProcessBuilder(
                                "python3",
                                "-m",
                                "langstream_grpc",
                                "[::]:%s".formatted(port),
                                MAPPER.writeValueAsString(configuration))
                        .inheritIO()
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.environment().put("PYTHONPATH", newPythonPath);
        processBuilder.environment().put("NLTK_DATA", "/app/nltk_data");
        pythonProcess = processBuilder.start();
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress("localhost", port)
                        .directExecutor()
                        .usePlaintext()
                        .build();
        AgentServiceGrpc.AgentServiceBlockingStub stub =
                AgentServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
        for (int i = 0; ; i++) {
            try {
                stub.agentInfo(Empty.getDefaultInstance());
                break;
            } catch (Exception e) {
                if (i > 8) {
                    throw e;
                }
                log.info("Waiting for python agent to start");
                Thread.sleep(1000);
            }
        }
        return channel;
    }

    public void close() throws Exception {
        if (pythonProcess != null) {
            pythonProcess.destroy();
            int exitCode = pythonProcess.waitFor();
            log.info("Python process exited with code {}", exitCode);

            if (exitCode != 0) {
                throw new RuntimeException("Python code exited with code " + exitCode);
            }
        }
    }
}
