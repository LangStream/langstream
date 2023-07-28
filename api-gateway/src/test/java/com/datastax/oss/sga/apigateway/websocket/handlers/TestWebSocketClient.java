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
import lombok.extern.slf4j.Slf4j;

@ClientEndpoint
@Slf4j
public class TestWebSocketClient implements AutoCloseable {


    public static final Handler NOOP = new Handler() {
        @Override
        public void onMessage(String msg) {

        }

        @Override
        public void onClose(CloseReason closeReason) {

        }

        @Override
        public void onError(Throwable throwable) {

        }
    };


    public interface Handler {

        default void onOpen(Session session) {
            log.info("onOpen client: {}", session.getId());
        }
        void onMessage(String msg);

        void onClose(CloseReason closeReason);

        default void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
        };
    }

    protected WebSocketContainer container;
    protected Session userSession = null;
    private final Handler eventHandler;


    public TestWebSocketClient(Handler eventHandler) {
        this.eventHandler = eventHandler;
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
        eventHandler.onOpen(session);
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        eventHandler.onClose(closeReason);
    }

    @OnMessage
    public void onMessage(String msg) {
        eventHandler.onMessage(msg);
    }

    @OnError
    public void onError(Throwable throwable) {
        eventHandler.onError(throwable);
    }

    @Override
    public void close() throws Exception {
        if (userSession != null) {
            userSession.close();
        }
    }
}
