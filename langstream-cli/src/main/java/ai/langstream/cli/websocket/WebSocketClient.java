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
package ai.langstream.cli.websocket;

import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.ClientEndpointConfig;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.Endpoint;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.websocket.Constants;
import org.apache.tomcat.websocket.pojo.PojoEndpointClient;

@ClientEndpoint
@Slf4j
public class WebSocketClient implements AutoCloseable {

    public interface Handler {
        default void onOpen() {}

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

    public WebSocketClient connect(URI uri) {
        return connect(uri, null);
    }

    @SneakyThrows
    public WebSocketClient connect(URI uri, Duration timeout) {
        try {
            final Endpoint endpoint = new PojoEndpointClient(this, List.of(), null);

            final ClientEndpointConfig clientEndpointConfig =
                    ClientEndpointConfig.Builder.create().build();
            if (timeout != null) {
                clientEndpointConfig
                        .getUserProperties()
                        .put(Constants.IO_TIMEOUT_MS_PROPERTY, timeout.toMillis() + "");
            }
            userSession = container.connectToServer(endpoint, clientEndpointConfig, uri);
        } catch (DeploymentException e) {
            String message = e.getMessage();
            if (Objects.equals(
                    "The HTTP response from the server [403] did not permit the HTTP upgrade to WebSocket",
                    message)) {
                throw new DeploymentException(
                        "The server answered 403 (forbidden), it is very likely that you are passing a wrong token (credentials parameter) "
                                + "or that the server requires authentication ",
                        e);
            }
            throw e;
        }
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
