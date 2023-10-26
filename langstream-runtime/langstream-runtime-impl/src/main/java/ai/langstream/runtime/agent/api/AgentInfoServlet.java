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
package ai.langstream.runtime.agent.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentInfoServlet extends HttpServlet {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AgentAPIController agentAPIController;

    public AgentInfoServlet(AgentAPIController agentAPIController) {
        this.agentAPIController = agentAPIController;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        MAPPER.writeValue(resp.getOutputStream(), agentAPIController.serveWorkerStatus());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        String uri = req.getRequestURI();
        if (uri.endsWith("/restart")) {
            try {
                MAPPER.writeValue(resp.getOutputStream(), agentAPIController.restart());
            } catch (Throwable error) {
                log.error("Error while restarting the agents");
                resp.getOutputStream().write((error + "").getBytes());
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        } else {
            resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
    }
}
