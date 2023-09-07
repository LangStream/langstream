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
package ai.langstream.apigateway.websocket.handlers;

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
public class TestWebSocketClient implements AutoCloseable {

    public static final Handler NOOP =
            new Handler() {
                @Override
                public void onMessage(String msg) {}

                @Override
                public void onClose(CloseReason closeReason) {}

                @Override
                public void onError(Throwable throwable) {}
            };

    public interface Handler {

        default void onOpen(Session session) {
            log.info("onOpen client: {}", session.getId());
        }

        default void onMessage(String msg) {}
        ;

        default void onClose(CloseReason closeReason) {}
        ;

        default void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
        }
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
