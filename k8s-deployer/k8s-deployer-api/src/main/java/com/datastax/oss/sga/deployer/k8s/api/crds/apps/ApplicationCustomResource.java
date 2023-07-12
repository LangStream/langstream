package com.datastax.oss.sga.deployer.k8s.api.crds.apps;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("sga.oss.datastax.com")
@Kind("Application")
@Singular("application")
@Plural("applications")
@ShortNames({"app"})
public class ApplicationCustomResource extends CustomResource<ApplicationSpec, ApplicationStatus> implements Namespaced {

    @Override
    protected ApplicationStatus initStatus() {
        return new ApplicationStatus();
    }

}
