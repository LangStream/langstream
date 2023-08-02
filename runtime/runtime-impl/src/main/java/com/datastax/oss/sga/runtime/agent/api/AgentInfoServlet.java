package com.datastax.oss.sga.runtime.agent.api;


import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class AgentInfoServlet extends HttpServlet {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AgentInfo agentInfo;

    public AgentInfoServlet(AgentInfo agentInfo) {
        this.agentInfo = agentInfo;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        MAPPER.writeValue(resp.getOutputStream(), agentInfo.serveInfos());
    }
}
