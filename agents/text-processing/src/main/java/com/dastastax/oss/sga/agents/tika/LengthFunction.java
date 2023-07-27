package com.dastastax.oss.sga.agents.tika;

public interface LengthFunction {

    /**
     * Calculate the length of the text.
     * It may be for example the number of characters or the number of words or tokens.
     * @param text
     * @return the length
     */
    int length(String text);
}
