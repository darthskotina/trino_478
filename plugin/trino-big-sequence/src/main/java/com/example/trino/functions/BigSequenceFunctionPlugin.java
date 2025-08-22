package com.example.trino.functions;

import io.trino.spi.Plugin;

import java.util.Set;

public class BigSequenceFunctionPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(BigSequenceFunction.class);
    }
}