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

import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.UUID;

public class PythonGrpcAgentProcessor extends GrpcAgentProcessor {

    private String className;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        className = (String) configuration.get("className");
    }

    @Override
    public void start() {
        String target = "unix:///tmp/%s.sock".formatted(UUID.randomUUID());
        this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        // TODO: start the Python server
        super.start();
    }
}
