package ai.langstream.agents.grpc;

import io.grpc.ManagedChannelBuilder;
import java.util.UUID;

public class PythonGrpcAgentProcessor extends GrpcAgentProcessor {

    @Override
    public void start() {
        String target = "uds:///tmp/%s.sock".formatted(UUID.randomUUID());
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        // TODO: start the Python server
        super.start();
    }
}
