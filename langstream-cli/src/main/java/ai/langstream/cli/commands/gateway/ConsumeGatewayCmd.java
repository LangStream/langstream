/**
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
package ai.langstream.cli.commands.gateway;

import ai.langstream.cli.websocket.WebSocketClient;
import jakarta.websocket.CloseReason;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "consume",
        description = "Consume messages from a gateway")
public class ConsumeGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @CommandLine.Option(names = {"-p", "--param"}, description = "Gateway parameters. Format: key=value")
    private Map<String, String> params;
    @CommandLine.Option(names = {"-c", "--credentials"}, description = "Credentials for the gateway. Required if the gateway requires authentication.")
    private String credentials;

    @CommandLine.Option(names = {"--position"}, description = "Initial position of the consumer. \"latest\", \"earliest\" or a offset value. "
            + "The offset value can be retrieved after consuming a message of the same topic.")
    private String initialPosition;

    @CommandLine.Option(names = {"-n", "--num-messages"}, description = "Number of messages to wait. Negative or zero means infinite.")
    private int numMessages = 0;



    @Override
    @SneakyThrows
    public void run() {
        Map<String, String> options = new HashMap<>();
        if (initialPosition != null) {
            options.put("position", initialPosition);
        }


        final String consumePath = "%s/v1/consume/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, gatewayId,
                        computeQueryString(credentials, params, options));

        final Integer expectedMessages = numMessages > 0 ? numMessages : null;
        final CountDownLatch latch;
        if (numMessages > 0) {
            latch = new CountDownLatch(numMessages);
        }else {
            latch = new CountDownLatch(1);
        }
        try (final WebSocketClient client = new WebSocketClient(new WebSocketClient.Handler() {
            @Override
            public void onMessage(String msg) {
                if (latch.getCount() > 0) {
                    if (expectedMessages != null) {
                        latch.countDown();
                    }
                    log(msg);
                }
            }

            @Override
            public void onClose(CloseReason closeReason) {
                exit();
            }

            private void exit() {
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log("Connection error: %s".formatted(throwable.getMessage()));
                exit();
            }
        })
                .connect(URI.create(consumePath))) {
            log("Connected to %s".formatted(consumePath));
            latch.await();
        }
    }
}
