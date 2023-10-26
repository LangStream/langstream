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

import java.util.Map;

public class PythonGrpcAgentProcessor extends GrpcAgentProcessor {

    private PythonGrpcServer server;
    private Map<String, Object> configuration;

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        super.init(configuration);
        this.configuration = configuration;
    }

    @Override
    public void start() throws Exception {
        server =
                new PythonGrpcServer(
                        agentContext.getCodeDirectory(), configuration, agentId(), agentContext);
        channel = server.start();
        super.start();
    }

    @Override
    public synchronized void close() throws Exception {
        stop();
    }

    @Override
    protected synchronized void stop() throws Exception {
        if (server != null) server.close();
    }

    @Override
    public void restart() throws Exception {
        stop();
        super.stop();

        super.start();
        start();
    }
}
