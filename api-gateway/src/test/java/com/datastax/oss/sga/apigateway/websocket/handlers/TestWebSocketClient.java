package com.datastax.oss.sga.apigateway.websocket.handlers;

import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.net.URI;
import java.util.function.Consumer;
import lombok.SneakyThrows;

@ClientEndpoint
public class TestWebSocketClient implements AutoCloseable {

    protected WebSocketContainer container;
    protected Session userSession = null;
    private final Consumer<String> onMessage;

    public TestWebSocketClient(Consumer<String> onMessage) {
        this.onMessage = onMessage;
        container = ContainerProvider.getWebSocketContainer();
    }

    @SneakyThrows
    public TestWebSocketClient connect(URI uri) {
        userSession = container.connectToServer(this, uri);
        return this;
    }

    @SneakyThrows
    public void send(String message) {
        userSession.getBasicRemote().sendText(message);
    }

    @OnOpen
    public void onOpen(Session session) {
        System.out.println("onOpen" + session);
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        System.out.println("onClose" + closeReason);
    }

    @OnMessage
    public void onMessage(String msg) {
        onMessage.accept(msg);
    }

    @OnError
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void close() throws Exception {
        if (userSession != null) {
            userSession.close();
        }
    }
}
