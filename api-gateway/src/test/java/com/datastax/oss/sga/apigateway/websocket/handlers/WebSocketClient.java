package com.datastax.oss.sga.apigateway.websocket.handlers;

import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Consumer;
import lombok.SneakyThrows;

@ClientEndpoint
public class WebSocketClient implements AutoCloseable {

    protected WebSocketContainer container;
    protected Session userSession = null;
    private final Consumer<String> onMessage;

    public WebSocketClient(Consumer<String> onMessage) {
        this.onMessage = onMessage;
        container = ContainerProvider.getWebSocketContainer();
    }

    @SneakyThrows
    public WebSocketClient connect(URI uri) {
        userSession = container.connectToServer(this, uri);
        return this;
    }

    @SneakyThrows
    public void send(String message) {
        userSession.getBasicRemote().sendText(message);
    }

    @OnOpen
    public void onOpen(Session session) {
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
    }

    @OnMessage
    public void onMessage(Session session, String msg) {
        onMessage.accept(msg);
    }

    @Override
    public void close() throws Exception {
        if (userSession != null) {
            userSession.close();
        }
    }
}
