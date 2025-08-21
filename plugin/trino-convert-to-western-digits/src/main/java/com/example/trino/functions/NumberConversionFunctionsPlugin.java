package com.example.trino;

import io.trino.spi.Plugin;

import java.util.Set;

public class NumberConversionFunctionsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(com.example.trino.functions.NumberConversionFunctions.class);
    }
}
