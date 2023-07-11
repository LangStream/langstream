package com.datastax.oss.sga.deployer.k8s.controllers;

import com.datastax.oss.sga.deployer.k8s.DeployerConfiguration;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.Agent;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.inject.Inject;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(namespaces = Constants.WATCH_ALL_NAMESPACES, name = "agent-controller")
@JBossLog
public class AgentController implements Reconciler<Agent> {

    @Inject
    KubernetesClient client;

    @Inject
    DeployerConfiguration configuration;

    @Override
    public UpdateControl<Agent> reconcile(Agent application, Context<Agent> context)
            throws Exception {

        log.infof("Got agent: %s, doing nothing..", application.getMetadata().getName());
        return UpdateControl.noUpdate();
    }

}
