package com.example.trino.functions;

import io.trino.spi.Plugin;

import java.util.Set;

public class RandomFunctionsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(RandomFunctions.class);
    }
}
