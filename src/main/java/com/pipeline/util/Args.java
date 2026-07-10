package com.pipeline.util;

import java.util.Optional;

/**
 * Minimal utility for reading positional CLI arguments and environment variables.
 * All methods return a default value when the argument is missing or unparseable.
 */
public final class Args {

    private Args() {} // utility class

    /**
     * Returns the positional argument at {@code index}, or {@code defaultValue}
     * if the index is out of bounds.
     *
     * @param args         the CLI args array
     * @param index        zero-based position
     * @param defaultValue fallback value
     * @return the argument or default
     */
    public static String get(String[] args, int index, String defaultValue) {
        return (args != null && index >= 0 && index < args.length && args[index] != null)
                ? args[index]
                : defaultValue;
    }

    /**
     * Parses the positional argument at {@code index} as an int,
     * or returns {@code defaultValue} if missing or unparseable.
     *
     * @param args         the CLI args array
     * @param index        zero-based position
     * @param defaultValue fallback value
     * @return the parsed int or default
     */
    public static int getInt(String[] args, int index, int defaultValue) {
        String raw = get(args, index, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.strip());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Parses the positional argument at {@code index} as a long,
     * or returns {@code defaultValue} if missing or unparseable.
     *
     * @param args         the CLI args array
     * @param index        zero-based position
     * @param defaultValue fallback value
     * @return the parsed long or default
     */
    public static long getLong(String[] args, int index, long defaultValue) {
        String raw = get(args, index, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.strip());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Returns the value of the environment variable {@code name},
     * or {@code defaultValue} if the variable is not set or is blank.
     *
     * @param name         environment variable name
     * @param defaultValue fallback value
     * @return the env value or default
     */
    public static String env(String name, String defaultValue) {
        return Optional.ofNullable(System.getenv(name))
                .filter(val -> !val.isBlank())
                .orElse(defaultValue);
    }
}
