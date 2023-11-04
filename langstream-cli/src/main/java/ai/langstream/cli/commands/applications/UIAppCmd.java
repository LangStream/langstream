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
package ai.langstream.cli.commands.applications;

import ai.langstream.cli.CLILogger;
import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.api.model.Gateways;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.protocols.ssl.UndertowXnioSsl;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.proxy.LoadBalancingProxyClient;
import io.undertow.server.handlers.proxy.ProxyHandler;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.xnio.OptionMap;
import org.xnio.Xnio;
import picocli.CommandLine;

@CommandLine.Command(name = "ui", header = "Run UI for interact with the application")
@Slf4j
public class UIAppCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "Port for the local webserver and UI. If 0, a random port will be used. ",
            defaultValue = "8092")
    private int port = 8092;

    @Override
    @SneakyThrows
    public void run() {
        final String applicationContent = getAppDescriptionOrLoad(applicationId);
        final List<Gateways.Gateway> gateways =
                Gateways.readFromApplicationDescription(applicationContent);

        final AppModel appModel = new AppModel();
        appModel.setApplicationDefinition(applicationContent);
        appModel.setMermaidDefinition(MermaidAppDiagramGenerator.generate(applicationContent));
        appModel.setApplicationId(applicationId);
        appModel.setGateways(gateways);
        final NamedProfile currentProfile = getCurrentProfile();
        appModel.setTenant(currentProfile.getTenant());
        final String apiGatewayUrl = currentProfile.getApiGatewayUrl();
        appModel.setRemoteBaseUrl(apiGatewayUrl);

        final LogSupplier logSupplier =
                new LogSupplier() {
                    @Override
                    @SneakyThrows
                    public void run(Consumer<String> lineConsumer) {
                        final HttpResponse<InputStream> response =
                                getClient().applications().logs(applicationId, List.of(), "text");
                        InputStream inputStream = response.body();
                        InputStreamReader reader =
                                new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                        BufferedReader bufferedReader = new BufferedReader(reader);

                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            lineConsumer.accept(line);
                        }
                    }
                };
        startServer(port, () -> appModel, apiGatewayUrl, logSupplier, getLogger());
        Thread.sleep(Long.MAX_VALUE);
    }

    @SneakyThrows
    public static Undertow startServer(
            int port,
            Supplier<AppModel> appModel,
            String apiGatewayUrl,
            LogSupplier logsStream,
            CLILogger logger) {
        String forwardHost;
        if (apiGatewayUrl.startsWith("wss://")) {
            forwardHost = "https://" + apiGatewayUrl.substring("wss://".length());
        } else {
            forwardHost = "http://" + apiGatewayUrl.substring("ws://".length());
        }
        if (forwardHost.endsWith("/")) {
            forwardHost = forwardHost.substring(0, forwardHost.length() - 1);
        }
        LoadBalancingProxyClient loadBalancer =
                new LoadBalancingProxyClient()
                        .addHost(
                                new URI(forwardHost),
                                forwardHost.startsWith("https://")
                                        ? new UndertowXnioSsl(
                                                Xnio.getInstance(), OptionMap.builder().getMap())
                                        : null)
                        .setConnectionsPerThread(20);

        final ProxyHandler proxyHandler =
                ProxyHandler.builder()
                        .setProxyClient(loadBalancer)
                        .setMaxRequestTime(30000)
                        .build();

        final HttpHandler logsHandler = new LogsHandler(logsStream);

        HttpHandler blockingHandler =
                exchange -> {
                    exchange.startBlocking();
                    if (exchange.isInIoThread()) {
                        exchange.dispatch(logsHandler);
                    } else {
                        logsHandler.handleRequest(exchange);
                    }
                };

        AtomicInteger actualPort = new AtomicInteger();

        ResourceHandler resourceHandler =
                Handlers.resource(
                        new ClassPathResourceManager(UIAppCmd.class.getClassLoader(), "app-ui"));
        HttpHandler appConfigHandler =
                exchange -> {
                    final AppModel result = appModel.get();
                    result.setBaseUrl("ws://localhost:" + actualPort.get());
                    final String json = jsonBodyWriter.writeValueAsString(result);
                    exchange.getResponseHeaders()
                            .put(HttpString.tryFromString("Content-Type"), "application/json");
                    exchange.getResponseSender().send(json);
                };
        RoutingHandler routingHandler =
                Handlers.routing()
                        .get("/api/application", appConfigHandler)
                        .get("/api/logs", blockingHandler)
                        .get("/v1/*", proxyHandler)
                        .setFallbackHandler(resourceHandler);

        Undertow server =
                Undertow.builder()
                        .addHttpListener(port, "0.0.0.0")
                        .setHandler(routingHandler)
                        .build();
        server.start();
        actualPort.set(
                ((InetSocketAddress) server.getListenerInfo().get(0).getAddress()).getPort());

        logger.log("Starting UI at http://localhost:" + actualPort.get());
        String os = System.getProperty("os.name").toLowerCase();
        logger.log("Operating system identified as: " + os);
        if (openBrowserAtPort("http://localhost:", actualPort.get())) {
            logger.log("Started UI at http://localhost:" + actualPort.get());
        } else {
            logger.log(
                    "Could not Start browser.  Either add the proper command to your OS (open for mac or xdg-open for linux) or start a browser manually at http://localhost:"
                            + actualPort.get());
        }

        return server;
    }

    static boolean checkAndLaunch(String openCommand, int port) {
        File f = new File(openCommand);
        boolean existsInFilesystem = f.exists();
        if (existsInFilesystem) {
            try {
                new ProcessBuilder(openCommand, "http://localhost:" + port).start();
                Thread.sleep(1000);
                return true;
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                return false;
            } catch (IOException ioException) {
                return false;
            }
        } else {
            return false;
        }
    }

    static boolean openBrowserAtPort(String URL, int port) {
        String openCommand = "";
        if (SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_WINDOWS) {
            Desktop desktop = Desktop.getDesktop();
            try {
                desktop.browse(new URI(URL + port));
            } catch (Exception e) {
                return false;
            }
            return true;
        } else if (SystemUtils.IS_OS_LINUX) {
            openCommand = "/usr/bin/xdg-open";
            return checkAndLaunch(openCommand, port);
        }
        return false;
    }

    @Data
    public static class AppModel {
        private String baseUrl;
        private String remoteBaseUrl;
        private String tenant;
        private String applicationId;
        private List<Gateways.Gateway> gateways;
        private String applicationDefinition;
        private String mermaidDefinition;
    }

    public interface LogSupplier {
        void run(Consumer<String> line);
    }

    @AllArgsConstructor
    private static class LogsHandler implements HttpHandler {

        private final LogSupplier logSupplier;

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.getResponseHeaders()
                    .put(Headers.CONTENT_TYPE, "text/plain; charset=" + StandardCharsets.UTF_8);
            final byte[] bytes = "\n".getBytes(StandardCharsets.UTF_8);

            logSupplier.run(
                    line -> {
                        try {
                            exchange.getOutputStream().write(line.getBytes(StandardCharsets.UTF_8));
                            exchange.getOutputStream().write(bytes);
                            exchange.getOutputStream().flush();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            exchange.endExchange();
        }
    }
}
