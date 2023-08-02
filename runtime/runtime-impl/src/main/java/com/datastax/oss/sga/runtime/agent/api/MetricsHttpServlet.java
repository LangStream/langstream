package com.datastax.oss.sga.runtime.agent.api;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.servlet.common.adapter.HttpServletRequestAdapter;
import io.prometheus.client.servlet.common.adapter.HttpServletResponseAdapter;
import io.prometheus.client.servlet.common.exporter.Exporter;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

public class MetricsHttpServlet extends HttpServlet {
    private final Exporter exporter = new Exporter(CollectorRegistry.defaultRegistry, (s) -> true);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        exporter.doGet(new HttpServletRequestAdapter() {
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
        }, new HttpServletResponseAdapter() {
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
