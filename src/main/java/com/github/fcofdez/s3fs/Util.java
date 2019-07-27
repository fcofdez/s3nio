package com.github.fcofdez.s3fs;

public class Util {

    public static void checkArgument(boolean value) {
        checkArgument(value, "");
    }

    public static void checkArgument(boolean value, String errorMessage, Object... errorParams) {
        if (!value)
            throw new IllegalArgumentException(String.format(errorMessage, errorParams));
    }

    public static boolean nullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }
}
