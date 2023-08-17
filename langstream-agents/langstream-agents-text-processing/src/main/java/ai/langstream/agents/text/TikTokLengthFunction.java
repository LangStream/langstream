/**
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.agents.text;

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
