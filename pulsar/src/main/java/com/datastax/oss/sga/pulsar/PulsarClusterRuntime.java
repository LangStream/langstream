package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;

import java.util.List;

@Slf4j
public class PulsarClusterRuntime implements ClusterRuntime<PulsarPhysicalApplicationInstance> {
    @Override
    public PulsarPhysicalApplicationInstance createImplementation(ApplicationInstance applicationInstance) {
        PulsarPhysicalApplicationInstance result = new PulsarPhysicalApplicationInstance();
        StreamingCluster streamingCluster = applicationInstance.getInstance().streamingCluster();

        String tenant = (String) streamingCluster.configuration().getOrDefault("defaultTenant", "public");
        String namespace = (String) streamingCluster.configuration().getOrDefault("defaultNamespace", "default");

        for (Module module : applicationInstance.getModules().values()) {
            for (TopicDefinition topic : module.getTopics().values()) {
                PulsarPhysicalApplicationInstance.PulsarName topicName
                        = new PulsarPhysicalApplicationInstance.PulsarName(tenant, namespace, topic.name());
                SchemaDefinition schema = topic.schema();
                String schemaType = schema != null ? schema.type() : null;
                String schemaDefinition = schema != null ? schema.schema() : null;
                PulsarPhysicalApplicationInstance.PulsarTopic pulsarTopic = new PulsarPhysicalApplicationInstance.PulsarTopic(topicName,
                        schemaType,
                        schemaDefinition,
                        topic.creationMode());
                result.getTopics().put(topicName, pulsarTopic);
            }
        }


        return result;
    }

    private PulsarAdmin buidPulsarAdmin(StreamingCluster streamingCluster) throws Exception {
        return PulsarAdmin
                .builder()
                .serviceHttpUrl((String) streamingCluster.configuration().getOrDefault("webServiceUrl", "http://localhost:8080"))
                .build();
    }

    @Override
    @SneakyThrows
    public void deploy(ApplicationInstance logicalInstance, PulsarPhysicalApplicationInstance applicationInstance) {
        try (PulsarAdmin admin  = buidPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (PulsarPhysicalApplicationInstance.PulsarTopic topic : applicationInstance.getTopics().values()) {
                String createMode = topic.createMode();
                switch (createMode) {
                    case "create-if-not-exists": {
                        String namespace = topic.name().tenant() + "/" + topic.name().namespace();
                        String topicName = topic.name().tenant() + "/" + topic.name().namespace() + "/" + topic.name().name();
                        List<String> existing = admin.topics().getList(namespace);
                        log.info("Existing topics: {}", existing);
                        if (!existing.contains(topicName)) {
                            log.info("Topic {} does not exist, creating", topicName);
                            admin
                                    .topics().createNonPartitionedTopic(topicName);
                        }
                    }
                }

            }
        }
    }

    @Override
    public void delete(ApplicationInstance logicalInstance, PulsarPhysicalApplicationInstance applicationInstance) {
        throw new UnsupportedOperationException();
    }
}
