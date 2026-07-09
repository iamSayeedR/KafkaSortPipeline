package com.pipeline.generate;

import com.pipeline.model.Record;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Factory that generates random {@link Record} instances.
 * Thread-safe — each call uses {@link ThreadLocalRandom#current()}.
 */
public class RandomRecordFactory {

    private static final String[] CONTINENTS = {
            "Africa", "Asia", "Australia", "Europe", "North America", "South America"
    };

    /** Characters for name generation: [A-Za-z] */
    private static final String ALPHA =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    /** Characters for address generation: [A-Za-z0-9 ] (space included) */
    private static final String ALPHANUM_SPACE =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789    ";

    /** Characters for first/last char of address (no space — avoids leading/trailing spaces) */
    private static final String ALPHANUM =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * Generates a single random record.
     *
     * @return a new {@link Record} with random field values
     */
    public Record nextRecord() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        int id = rng.nextInt(); // full int32 range including negatives
        String name = randomString(rng, ALPHA, 10, 15);
        String address = randomAddress(rng, 15, 20);
        String continent = CONTINENTS[rng.nextInt(CONTINENTS.length)];

        return new Record(id, name, address, continent);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /**
     * Generates a random string of length in [minLen, maxLen] from the given alphabet.
     */
    private static String randomString(ThreadLocalRandom rng, String alphabet, int minLen, int maxLen) {
        int len = rng.nextInt(minLen, maxLen + 1); // inclusive upper bound
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = alphabet.charAt(rng.nextInt(alphabet.length()));
        }
        return new String(chars);
    }

    /**
     * Generates a random address string of length in [minLen, maxLen].
     * First and last characters are never spaces.
     */
    private static String randomAddress(ThreadLocalRandom rng, int minLen, int maxLen) {
        int len = rng.nextInt(minLen, maxLen + 1);
        if (len == 0) return "";

        char[] chars = new char[len];
        // First char: no space
        chars[0] = ALPHANUM.charAt(rng.nextInt(ALPHANUM.length()));
        // Middle chars: may include spaces
        for (int i = 1; i < len - 1; i++) {
            chars[i] = ALPHANUM_SPACE.charAt(rng.nextInt(ALPHANUM_SPACE.length()));
        }
        // Last char: no space (only if length > 1)
        if (len > 1) {
            chars[len - 1] = ALPHANUM.charAt(rng.nextInt(ALPHANUM.length()));
        }
        return new String(chars);
    }
}
