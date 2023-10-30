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

import ai.langstream.api.runner.code.AgentService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAgentService extends AbstractGrpcAgent implements AgentService {

    public GrpcAgentService() {
        super();
    }

    @Override
    public void start() throws Exception {
        super.start();
        restarting.set(false);
        startFailedButDevelopmentMode = false;
    }

    protected void stopBeforeRestart() throws Exception {
        log.info("Stopping...");
        restarting.set(true);
        super.stopBeforeRestart();
        log.info("Stopped");
    }

    @Override
    public void onNewSchemaToSend(Schema schema) {}

    @Override
    public void join() throws Exception {
        Thread.sleep(Long.MAX_VALUE);
    }
}
