package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.CONSUME_PATH;
import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.PRODUCE_PATH;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
public class ProduceHandler extends AbstractHandler {

    public ProduceHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    private TopicProducer producer;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            final AntPathMatcher antPathMatcher = new AntPathMatcher();
            final Map<String, String> vars =
                    antPathMatcher.extractUriTemplateVariables(PRODUCE_PATH,
                            session.getUri().getPath());
            session.getAttributes().put("application", vars.get("application"));
            final String topic = vars.get("topic");
            session.getAttributes().put("topic", topic);

            final String tenant = (String) session.getAttributes().get("tenant");
            final String applicationId = (String) session.getAttributes().get("application");


            final StoredApplication application = applicationStore.get(tenant, applicationId);
            TopicDefinition topicDefinition = null;
            for (com.datastax.oss.sga.api.model.Module module : application.getInstance().getModules().values()) {
                final TopicDefinition moduleTopicDef = module.getTopics().get(topic);
                if (moduleTopicDef != null) {
                    topicDefinition = moduleTopicDef;
                    break;
                }
            }
            if (topicDefinition == null) {
                throw new IllegalArgumentException("topic" + topic + " is not defined in the application");
            }
            final StreamingCluster streamingCluster = application
                    .getInstance()
                    .getInstance().streamingCluster();
            final TopicConnectionsRuntime topicConnectionsRuntime =
                    TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

            producer =
                    topicConnectionsRuntime.createProducer("ag-" + session.getId(), streamingCluster,
                            Map.of("topic", topicDefinition.getName()));
            producer.start();
        } catch (Throwable tt) {
            log.error("Error while establishing connection", tt);
            session.close();
        }
    }

    @Override
    @SneakyThrows
    public void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            producer.write(List.of(new com.datastax.oss.sga.api.runner.code.Record() {
                @Override
                public Object key() {
                    return null;
                }

                @Override
                public Object value() {
                    return message.getPayload();
                }

                @Override
                public String origin() {
                    return null;
                }

                @Override
                public Long timestamp() {
                    return null;
                }

                @Override
                public Collection<Header> headers() {
                    return null;
                }
            }));
        } catch (Throwable tt) {
            log.error("Error while writing message", tt);
            session.close();
        }
    }
}
