package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.datastax.oss.sga.api.model.ApplicationInstanceLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.api.crds.BaseStatus;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Data;

@Data
public class ApplicationStatus extends BaseStatus {

    ApplicationInstanceLifecycleStatus status;

}
