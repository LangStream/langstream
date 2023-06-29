package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class PulsarPhysicalApplicationInstance implements PhysicalApplicationInstance {

    record PulsarName(String tenant, String namespace, String name) { }
    record PulsarTopic(PulsarName name, String schemaType, String schema, String createMode) { }
    record PulsarFunction(PulsarName id, PulsarName inputTopic, PulsarName outputTopic, Map<String, Object> configuration) {}
    record PulsarSink(PulsarName name, PulsarName inputTopic, Map<String, Object> configuration) {}
    record PulsarSource(PulsarName name, PulsarName outputTopic, Map<String, Object> configuration) {}

    final Map<PulsarName, PulsarTopic> topics = new HashMap<>();
    final Map<PulsarName, PulsarFunction> functions = new HashMap<>();
    final Map<PulsarName, PulsarSink> sinks = new HashMap<>();
    final Map<PulsarName, PulsarSource> sources = new HashMap<>();


}
