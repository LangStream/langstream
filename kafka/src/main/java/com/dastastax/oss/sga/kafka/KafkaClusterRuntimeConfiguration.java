package com.dastastax.oss.sga.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaClusterRuntimeConfiguration {

    private Map<String, Object> admin;

}
