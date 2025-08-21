package io.trino.plugin.jwt;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class JwtFunctions {
    private JwtFunctions() {}

    @Description("Decodes the payload of a JWT token (base64url -> string)")
    @ScalarFunction("decode_jwt")
    @SqlType("varchar")
    public static Slice decodeJwt(@SqlType("varchar") Slice jwt) {
        String token = jwt.toStringUtf8();
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid JWT: must have 3 parts");
        }
        byte[] decoded = Base64.getUrlDecoder().decode(parts[1]);
        return Slices.utf8Slice(new String(decoded, StandardCharsets.UTF_8));
    }
}

