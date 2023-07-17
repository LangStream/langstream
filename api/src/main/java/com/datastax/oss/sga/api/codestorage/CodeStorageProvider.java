package com.datastax.oss.sga.api.codestorage;

import java.util.Map;

public interface CodeStorageProvider {

    /**
     * Create an Implementation of a CodeStorage implementation.
     * @param codeStorageType
     * @return the implementation
     */
    CodeStorage createImplementation(String codeStorageType, Map<String, Object> configuration);

    /**
     * Returns the ability of an Agent to be deployed on the give runtimes.
     * @param codeStorageType
     * @return true if this provider that can create the implementation
     */
    boolean supports(String codeStorageType);

}
