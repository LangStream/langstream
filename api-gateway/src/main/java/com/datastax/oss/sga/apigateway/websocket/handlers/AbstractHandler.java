package com.datastax.oss.sga.apigateway.websocket.handlers;

import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import lombok.AllArgsConstructor;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@AllArgsConstructor
public abstract class AbstractHandler extends TextWebSocketHandler {
    protected static final TopicConnectionsRuntimeRegistry TOPIC_CONNECTIONS_REGISTRY = new TopicConnectionsRuntimeRegistry();
    protected final ApplicationStore applicationStore;


}
