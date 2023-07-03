package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.ApplicationInstance;

public class PlaceholderEvaluator {

    public static ApplicationInstance evaluate(ApplicationInstance applicationInstance) {
        new ApplicationInstance();
        return applicationInstance;
    }

}
