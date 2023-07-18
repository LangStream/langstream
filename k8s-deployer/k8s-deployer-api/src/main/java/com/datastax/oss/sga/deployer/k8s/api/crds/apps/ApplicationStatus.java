package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import com.datastax.oss.sga.api.model.ApplicationLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.api.crds.BaseStatus;
import lombok.Data;

@Data
public class ApplicationStatus extends BaseStatus {

    ApplicationLifecycleStatus status;

}
