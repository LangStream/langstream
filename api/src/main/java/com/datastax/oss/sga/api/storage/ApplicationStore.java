package com.datastax.oss.sga.api.storage;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.model.StoredApplication;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface ApplicationStore extends GenericStore {

    void onTenantCreated(String tenant);

    void onTenantDeleted(String tenant);

    void put(String tenant, String applicationId, Application applicationInstance, String codeArchiveReference);

    StoredApplication get(String tenant, String applicationId);

    Application getSpecs(String tenant, String applicationId);

    Secrets getSecrets(String tenant, String applicationId);

    void delete(String tenant, String applicationId);

    Map<String, StoredApplication> list(String tenant);


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class LogOptions {
        private List<String> filterReplicas;
    }


    @FunctionalInterface
    interface PodLogHandler {
        void start(LogLineConsumer onLogLine);
    }

    @FunctionalInterface
    interface LogLineConsumer {
        boolean onLogLine(String line);
    }


    List<PodLogHandler> logs(String tenant, String applicationId, LogOptions logOptions);


}
