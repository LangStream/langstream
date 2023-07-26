package com.datastax.oss.sga.apigateway.websocket.handlers;

import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

@ClientEndpoint
public class WebSocketClient implements AutoCloseable {


    public static class W {
        @Test
        public void test() throws Exception {
            new WebSocketClient(msg -> {
                System.out.println("got message: " + msg);
            })
                    .connect(URI.create("ws://localhost:8091/v1/consume/default/gw/consume-input"));
            System.out.println("connected");
            Thread.sleep(Long.MAX_VALUE);

        }
    }

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
        System.out.println("onOpen" + session);
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        System.out.println("onOpen" + closeReason);
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
