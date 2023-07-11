package com.datastax.oss.sga.api.storage;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.StoredApplicationInstance;
import java.util.Map;

public interface ApplicationStore extends GenericStore {

    void initializeTenant(String tenant);

    void put(String tenant, String name, ApplicationInstance applicationInstance);

    StoredApplicationInstance get(String tenant, String name);

    void delete(String tenant, String name);

    Map<String, StoredApplicationInstance> list(String tenant);
}
