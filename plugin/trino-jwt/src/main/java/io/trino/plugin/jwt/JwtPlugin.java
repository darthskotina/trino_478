package io.trino.plugin.jwt;

import io.trino.spi.Plugin;
import java.util.Set;

public class JwtPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(JwtFunctions.class);
    }
}
