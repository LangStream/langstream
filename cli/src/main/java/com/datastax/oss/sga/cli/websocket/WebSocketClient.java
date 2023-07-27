package com.datastax.oss.sga.cli.websocket;

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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@ClientEndpoint
@Slf4j
public class WebSocketClient implements AutoCloseable {

    public interface Handler {
        default void onOpen() {};

        void onMessage(String msg);

        void onClose(CloseReason closeReason);

        void onError(Throwable throwable);
    }

    protected WebSocketContainer container;
    protected Session userSession = null;
    private final Handler eventHandler;

    public WebSocketClient(Handler eventHandler) {
        this.eventHandler = eventHandler;
        container = ContainerProvider.getWebSocketContainer();
        container.setDefaultMaxBinaryMessageBufferSize(32768);
        container.setDefaultMaxTextMessageBufferSize(32768);
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
        eventHandler.onOpen();
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        eventHandler.onClose(closeReason);
    }

    @OnError
    public void onError(Throwable throwable) {
        eventHandler.onError(throwable);
    }

    @OnMessage
    public void onMessage(Session session, String msg) {
        eventHandler.onMessage(msg);
    }

    @Override
    public void close() throws Exception {
        if (userSession != null) {
            userSession.close();
        }
    }
}
