package com.datastax.oss.sga.api.runner.topics;

import java.util.Map;

public record OffsetPerPartition(Map<String, String> offsets){}
