package ai.langstream.apigateway.gateways;

import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.api.storage.ApplicationStoreRegistry;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.config.StorageProperties;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import java.util.Objects;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayRequestHandlerFactory {

    @Bean
    public GatewayRequestHandler gatewayRequestHandler(ApplicationStore applicationStore,
                                                       GatewayTestAuthenticationProperties testAuthenticationProperties) {
        return new GatewayRequestHandler(applicationStore, testAuthenticationProperties);
    }

}
