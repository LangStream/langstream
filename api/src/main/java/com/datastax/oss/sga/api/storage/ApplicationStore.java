package com.datastax.oss.sga.api.storage;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.StoredApplication;
import java.util.Map;

public interface ApplicationStore extends GenericStore {

    void onTenantCreated(String tenant);

    void onTenantDeleted(String tenant);

    void put(String tenant, String name, Application applicationInstance);

    StoredApplication get(String tenant, String name);

    void delete(String tenant, String name);

    Map<String, StoredApplication> list(String tenant);
}
