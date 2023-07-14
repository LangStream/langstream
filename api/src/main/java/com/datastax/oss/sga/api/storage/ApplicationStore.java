package com.datastax.oss.sga.api.storage;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.StoredApplication;
import java.util.Map;

public interface ApplicationStore extends GenericStore {

    void onTenantCreated(String tenant);

    void onTenantDeleted(String tenant);

    void put(String tenant, String applicationId, Application applicationInstance);

    StoredApplication get(String tenant, String applicationId);

    void delete(String tenant, String applicationId);

    Map<String, StoredApplication> list(String tenant);
}
