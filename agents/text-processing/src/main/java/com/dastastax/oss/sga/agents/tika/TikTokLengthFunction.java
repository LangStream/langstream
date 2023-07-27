package com.dastastax.oss.sga.agents.tika;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;


public class TikTokLengthFunction implements LengthFunction {

    private final static EncodingRegistry REGISTRY = Encodings.newDefaultEncodingRegistry();
    private final EncodingType encodingType;

    public TikTokLengthFunction(String encoding) {
        encodingType = EncodingType.fromName(encoding)
                .orElseThrow(() -> new IllegalArgumentException("Unknown encoding: " + encoding));
    }

    @Override
    public int length(String text) {
        // Encoding is stateful and it retains references to internal tokens
        Encoding enc = REGISTRY.getEncoding(encodingType);
        return enc.countTokens(text);
    }
}
