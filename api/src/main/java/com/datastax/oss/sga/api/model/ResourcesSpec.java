package com.datastax.oss.sga.api.model;

/**
 * Definition of the resources required by the agent.
 */
public record ResourcesSpec(Integer parallelism, Integer size) {

    public static ResourcesSpec DEFAULT = new ResourcesSpec(1, 1);

    public ResourcesSpec withDefaultsFrom(ResourcesSpec higherLevel){
        if (higherLevel == null) {
            return this;
        }
        Integer newParallelism = parallelism == null ? higherLevel.parallelism() : parallelism;
        Integer newUnits = size == null ? higherLevel.size() : size;
        return new ResourcesSpec(newParallelism, newUnits);
    }
}
