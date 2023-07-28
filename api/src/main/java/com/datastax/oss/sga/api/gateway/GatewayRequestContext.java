package com.datastax.oss.sga.api.gateway;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Gateway;
import java.util.Map;

public interface GatewayRequestContext {

    String tenant();

    String applicationId();

    Application application();

    Gateway gateway();

    String credentials();

    Map<String, String> userParameters();

    Map<String, String> options();
}
