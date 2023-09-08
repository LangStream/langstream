package ai.langstream.auth.jwt;

import java.util.List;

public record JwtProperties(
        String secretKey,
        String publicKey,
        String authClaim,
        String publicAlg,
        String audienceClaim,
        String audience,
        String jwksHostsAllowlist) {}
