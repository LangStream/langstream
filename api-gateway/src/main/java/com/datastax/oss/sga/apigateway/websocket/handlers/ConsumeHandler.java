package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.CONSUME_PATH;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ConsumeHandler extends AbstractHandler {

    public ConsumeHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            final AntPathMatcher antPathMatcher = new AntPathMatcher();
            final Map<String, String> vars =
                    antPathMatcher.extractUriTemplateVariables(CONSUME_PATH,
                            session.getUri().getPath());
            session.getAttributes().put("application", vars.get("application"));
            final String topic = vars.get("topic");
            session.getAttributes().put("topic", topic);

            final String tenant = (String) session.getAttributes().get("tenant");
            final String applicationId = (String) session.getAttributes().get("application");
            setupConsumer(session, topic, tenant, applicationId);
        } catch (Throwable tt) {
            log.error("Error while setting up consumer", tt);
            session.close();
        }
    }

    private void setupConsumer(WebSocketSession session, String topic, String tenant, String applicationId)
            throws Exception {
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


        final TopicConsumer consumer =
                topicConnectionsRuntime.createConsumer("ag-" + session.getId(), streamingCluster, Map.of("topic", topicDefinition.getName()));
        consumer.start();


        while (true) {
            final List<Record> records = consumer.read();
            for (Record record : records) {
                session.sendMessage(new TextMessage(String.valueOf(record.value())));
            }
        }
    }

    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) {

    }

}
