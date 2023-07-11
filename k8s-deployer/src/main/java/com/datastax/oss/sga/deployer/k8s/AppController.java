package com.datastax.oss.sga.deployer.k8s;

import com.datastax.oss.sga.deployer.k8s.crds.Application;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "app-controller")
@JBossLog
public class AppController implements Reconciler<Application> {

    @Override
    public UpdateControl<Application> reconcile(Application application, Context<Application> context)
            throws Exception {
        System.out.println("reconcile..."  + application.getSpec().getName());
        return UpdateControl.noUpdate();
    }
}
