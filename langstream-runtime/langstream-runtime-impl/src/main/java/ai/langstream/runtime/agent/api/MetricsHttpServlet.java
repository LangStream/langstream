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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.servlet.common.adapter.HttpServletRequestAdapter;
import io.prometheus.client.servlet.common.adapter.HttpServletResponseAdapter;
import io.prometheus.client.servlet.common.exporter.Exporter;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class MetricsHttpServlet extends HttpServlet {
    private final Exporter exporter = new Exporter(CollectorRegistry.defaultRegistry, (s) -> true);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        exporter.doGet(
                new HttpServletRequestAdapter() {
                    @Override
                    public String getHeader(String s) {
                        return req.getHeader(s);
                    }

                    @Override
                    public String getRequestURI() {
                        return req.getRequestURI();
                    }

                    @Override
                    public String getMethod() {
                        return req.getMethod();
                    }

                    @Override
                    public String[] getParameterValues(String s) {
                        return req.getParameterValues(s);
                    }

                    @Override
                    public String getContextPath() {
                        return req.getContextPath();
                    }
                },
                new HttpServletResponseAdapter() {
                    @Override
                    public int getStatus() {
                        return resp.getStatus();
                    }

                    @Override
                    public void setStatus(int i) {
                        resp.setStatus(i);
                    }

                    @Override
                    public void setContentType(String s) {
                        resp.setContentType(s);
                    }

                    @Override
                    public PrintWriter getWriter() throws IOException {
                        return resp.getWriter();
                    }
                });
    }
}
