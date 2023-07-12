package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.Application;

public class PlaceholderEvaluator {

    public static Application evaluate(Application applicationInstance) {
        new Application();
        return applicationInstance;
    }

}
