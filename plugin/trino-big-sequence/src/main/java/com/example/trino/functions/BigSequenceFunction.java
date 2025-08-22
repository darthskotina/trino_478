package com.example.trino.functions;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import static io.trino.spi.type.BigintType.BIGINT;

public final class BigSequenceFunction {
    private BigSequenceFunction() {}

    @ScalarFunction("big_sequence")
    @SqlType("array(bigint)")
    public static Block bigSequence(
            @SqlType("bigint") long start,
            @SqlType("bigint") long end)
    {
        if (end < start) {
            return BIGINT.createBlockBuilder(null, 0).build();
        }

        long size = end - start + 1;
        if (size > 100_000) {
            throw new IllegalArgumentException("Too many elements in sequence (max 100,000)");
        }

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, (int) size);
        for (long i = start; i <= end; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        return blockBuilder.build();
    }
}
