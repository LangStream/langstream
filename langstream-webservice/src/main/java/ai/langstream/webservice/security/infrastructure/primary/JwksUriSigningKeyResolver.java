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
package ai.langstream.webservice.security.infrastructure.primary;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.io.Decoders;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwksUriSigningKeyResolver implements SigningKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(JwksUriSigningKeyResolver.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String algorithm;
    private final Pattern hostsAllowlist;
    private final Key fallbackKey;
    private final Map<String, Key> keyMap = new ConcurrentHashMap<>();

    public JwksUriSigningKeyResolver(String algorithm, String hostsAllowlist, Key fallbackKey) {
        this.algorithm = algorithm;
        if (StringUtils.isBlank(hostsAllowlist)) {
            this.hostsAllowlist = null;
        } else {
            this.hostsAllowlist = Pattern.compile(hostsAllowlist);
        }
        this.fallbackKey = fallbackKey;
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, Claims claims) {
        final String jwksUri = (String) claims.get("jwks_uri");
        if (jwksUri == null) {
            return fallbackKey;
        }
        return getKey(jwksUri);
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, String plaintext) {
        throw new UnsupportedOperationException();
    }

    private Key getKey(String uri) {
        return keyMap.computeIfAbsent(uri, this::fetchKey);
    }

    private Key fetchKey(String uri) {
        try {
            final URL src = new URL(uri);
            if (hostsAllowlist == null || !hostsAllowlist.matcher(src.getHost()).matches()) {
                throw new JwtException("Untrusted hostname: '" + src.getHost() + "'");
            }
            final JwkKeys keys = MAPPER.readValue(src, JwkKeys.class);
            for (JwkKey key : keys.keys()) {
                if (!algorithm.equals(key.alg())) {
                    continue;
                }
                BigInteger modulus = base64ToBigInteger(key.n());
                BigInteger exponent = base64ToBigInteger(key.e());
                RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(modulus, exponent);
                try {
                    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                    return keyFactory.generatePublic(rsaPublicKeySpec);
                } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                    throw new IllegalStateException("Failed to parse public key");
                }
            }
            throw new JwtException("No valid keys found from URL: " + uri);
        } catch (IOException e) {
            System.out.println(e);
            log.error("Failed to fetch keys from URL: {}", uri, e);
            throw new JwtException("Failed to fetch keys from URL: " + uri, e);
        }
    }

    record JwkKeys(List<JwkKey> keys) {}

    record JwkKey(String alg, String e, String kid, String kty, String n) {}

    private BigInteger base64ToBigInteger(String value) {
        return new BigInteger(1, Decoders.BASE64URL.decode(value));
    }
}
