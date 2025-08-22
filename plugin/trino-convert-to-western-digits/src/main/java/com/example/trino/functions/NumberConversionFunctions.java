package com.example.trino.functions;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public class NumberConversionFunctions {

    @Description("Converts Arabic-Indic, Devanagari, or Bengali timestamp string to Western digits")
    @ScalarFunction("convert_to_western_digits")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertToWesternDigits(@SqlType(StandardTypes.VARCHAR) Slice input) {
        String inputStr = input.toStringUtf8();
        StringBuilder result = new StringBuilder();

        for (char ch : inputStr.toCharArray()) {
            if (ch >= '٠' && ch <= '٩') {  // Arabic-Indic
                result.append((char) ('0' + (ch - '٠')));
            } else if (ch >= '०' && ch <= '९') {  // Devanagari
                result.append((char) ('0' + (ch - '०')));
            } else if (ch >= '০' && ch <= '৯') {  // Bengali
                result.append((char) ('0' + (ch - '০')));
            } else {
                result.append(ch);  // keep colons, dashes, spaces, etc.
            }
        }

        return Slices.utf8Slice(result.toString());
    }
}
