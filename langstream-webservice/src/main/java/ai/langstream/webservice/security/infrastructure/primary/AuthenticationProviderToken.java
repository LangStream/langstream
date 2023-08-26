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

import ai.langstream.webservice.config.AuthTokenProperties;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.RequiredTypeException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.io.DecodingException;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.List;
import javax.crypto.SecretKey;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class AuthenticationProviderToken {

    public static final class AuthenticationException extends Exception {

        public AuthenticationException(String message) {
            super(message);
        }
    }

    private final JwtParser parser;
    private final String roleClaim;
    private final SignatureAlgorithm publicKeyAlg;
    private final String audienceClaim;
    private final String audience;

    public AuthenticationProviderToken(AuthTokenProperties tokenProperties) throws IOException, IllegalArgumentException {
        this.publicKeyAlg = getPublicKeyAlgType(tokenProperties);
        parser =
                Jwts
                        .parserBuilder()
                        .setSigningKeyResolver(
                                new JwksUriSigningKeyResolver(
                                        publicKeyAlg.getValue(),
                                        tokenProperties.jwksHostsAllowlist(),
                                        getValidationKeyFromConfig(tokenProperties)
                                )
                        )
                        .build();
        this.roleClaim = getTokenRoleClaim(tokenProperties);
        this.audienceClaim = getTokenAudienceClaim(tokenProperties);
        this.audience = getTokenAudience(tokenProperties);

        if (this.audienceClaim != null && this.audience == null) {
            throw new IllegalArgumentException(
                    "Token Audience Claim [" + this.audienceClaim + "] configured, but Audience stands for this broker not."
            );
        }
    }

    public String authenticate(String token) throws AuthenticationException {
        final Jwt<?, Claims> jwt = authenticateToken(token);
        return getPrincipal(jwt);
    }

    private Jwt<?, Claims> authenticateToken(final String token) throws AuthenticationException {
        try {
            Jwt<?, Claims> jwt = parser.parseClaimsJws(token);
            if (this.audienceClaim != null) {
                Object object = jwt.getBody().get(this.audienceClaim);
                if (object == null) {
                    throw new JwtException("Found null Audience in token, for claimed field: " + this.audienceClaim);
                }

                if (object instanceof List) {
                    List<String> audiences = (List) object;
                    if (
                            audiences
                                    .stream()
                                    .noneMatch(audienceInToken -> audienceInToken.equals(this.audience))
                    ) {
                        throw new AuthenticationException(
                                "Audiences in token: [" + String.join(", ", audiences) + "] not contains this broker: " + this.audience
                        );
                    }
                } else {
                    if (!(object instanceof String)) {
                        throw new AuthenticationException("Audiences in token is not in expected format: " + object);
                    }

                    if (!object.equals(this.audience)) {
                        throw new AuthenticationException("Audiences in token: [" + object + "] not contains this broker: " + this.audience);
                    }
                }
            }
            return jwt;
        } catch (JwtException ex) {
            throw new AuthenticationException("Failed to authentication token: " + ex.getMessage());
        }
    }

    private String getPrincipal(Jwt<?, Claims> jwt) {
        try {
            return jwt.getBody().get(this.roleClaim, String.class);
        } catch (RequiredTypeException var4) {
            List list = (jwt.getBody()).get(this.roleClaim, List.class);
            return list != null && !list.isEmpty() && list.get(0) instanceof String ? (String) list.get(0) : null;
        }
    }

    private Key getValidationKeyFromConfig(AuthTokenProperties tokenProperties) throws IOException {
        String tokenSecretKey = tokenProperties.secretKey();
        String tokenPublicKey = tokenProperties.publicKey();
        byte[] validationKey;
        if (StringUtils.isNotBlank(tokenSecretKey)) {
            validationKey = readKeyFromUrl(tokenSecretKey);
            return decodeSecretKey(validationKey);
        } else if (StringUtils.isNotBlank(tokenPublicKey)) {
            validationKey = readKeyFromUrl(tokenPublicKey);
            return decodePublicKey(validationKey, this.publicKeyAlg);
        }
        return null;
    }

    private static byte[] readKeyFromUrl(String keyConfUrl) throws IOException {
        if (!keyConfUrl.startsWith("data:") && !keyConfUrl.startsWith("file:")) {
            if (Files.exists(Paths.get(keyConfUrl))) {
                return Files.readAllBytes(Paths.get(keyConfUrl));
            } else if (Base64.isBase64(keyConfUrl.getBytes())) {
                try {
                    return Decoders.BASE64.decode(keyConfUrl);
                } catch (DecodingException var3) {
                    String msg = "Illegal base64 character or Key file " + keyConfUrl + " doesn't exist";
                    throw new IOException(msg, var3);
                }
            } else {
                String msg = "Secret/Public Key file " + keyConfUrl + " doesn't exist";
                throw new IllegalArgumentException(msg);
            }
        } else {
            try {
                return IOUtils.toByteArray(new URL(keyConfUrl));
            } catch (IOException var4) {
                throw var4;
            } catch (Exception var5) {
                throw new IOException(var5);
            }
        }
    }

    private static SecretKey decodeSecretKey(byte[] secretKey) {
        return Keys.hmacShaKeyFor(secretKey);
    }

    private String getTokenRoleClaim(AuthTokenProperties tokenProperties) {
        String tokenAuthClaim = tokenProperties.authClaim();
        return StringUtils.isNotBlank(tokenAuthClaim) ? tokenAuthClaim : "sub";
    }

    private SignatureAlgorithm getPublicKeyAlgType(AuthTokenProperties tokenProperties) throws IllegalArgumentException {
        String tokenPublicAlg = tokenProperties.publicAlg();
        if (StringUtils.isNotBlank(tokenPublicAlg)) {
            try {
                return SignatureAlgorithm.forName(tokenPublicAlg);
            } catch (SignatureException var4) {
                throw new IllegalArgumentException("invalid algorithm provided " + tokenPublicAlg, var4);
            }
        } else {
            return SignatureAlgorithm.RS256;
        }
    }

    private static PublicKey decodePublicKey(byte[] key, SignatureAlgorithm algType) throws IOException {
        try {
            X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
            KeyFactory kf = KeyFactory.getInstance(keyTypeForSignatureAlgorithm(algType));
            return kf.generatePublic(spec);
        } catch (Exception var4) {
            throw new IOException("Failed to decode public key", var4);
        }
    }

    private static String keyTypeForSignatureAlgorithm(SignatureAlgorithm alg) {
        if (alg.getFamilyName().equals("RSA")) {
            return "RSA";
        } else if (alg.getFamilyName().equals("ECDSA")) {
            return "EC";
        } else {
            String msg = "The " + alg.name() + " algorithm does not support Key Pairs.";
            throw new IllegalArgumentException(msg);
        }
    }

    private String getTokenAudienceClaim(AuthTokenProperties tokenProperties) throws IllegalArgumentException {
        String tokenAudienceClaim = tokenProperties.audienceClaim();
        return StringUtils.isNotBlank(tokenAudienceClaim) ? tokenAudienceClaim : null;
    }

    private String getTokenAudience(AuthTokenProperties tokenProperties) throws IllegalArgumentException {
        String tokenAudience = tokenProperties.audience();
        return StringUtils.isNotBlank(tokenAudience) ? tokenAudience : null;
    }
}
