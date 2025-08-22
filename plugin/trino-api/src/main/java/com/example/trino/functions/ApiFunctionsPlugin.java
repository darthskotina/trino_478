package com.example.trino.functions;

import io.trino.spi.Plugin;
import java.util.Set;

public class ApiFunctionsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(ApiFunctions.class);
    }
}
