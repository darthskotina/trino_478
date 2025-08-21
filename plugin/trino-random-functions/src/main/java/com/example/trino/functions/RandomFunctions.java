package com.example.trino.functions;

import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import java.util.Random;

public final class RandomFunctions {
    private RandomFunctions() {}

    @ScalarFunction("rand")
    @Description("Returns a pseudorandom double in [0.0, 1.0) using a seed")
    @SqlType("double")
    public static double rand(@SqlType("bigint") long seed) {
        return new Random(seed).nextDouble();
    }

    @ScalarFunction("randn")
    @Description("Returns a pseudorandom value from standard normal distribution using a seed")
    @SqlType("double")
    public static double randn(@SqlType("bigint") long seed) {
        return new Random(seed).nextGaussian();
    }
}
