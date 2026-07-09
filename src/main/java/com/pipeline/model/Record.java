package com.pipeline.model;

/**
 * Immutable data record representing a single row in the pipeline.
 * Uses a Java record for compact, value-semantic representation.
 */
public record Record(int id, String name, String address, String continent) {

    /**
     * Compact constructor — validates that no field is null.
     */
    public Record {
        java.util.Objects.requireNonNull(name, "name must not be null");
        java.util.Objects.requireNonNull(address, "address must not be null");
        java.util.Objects.requireNonNull(continent, "continent must not be null");
    }

    /**
     * Serializes this record to a simple CSV string.
     * Fields are guaranteed to contain no commas, so no quoting is needed.
     *
     * @return CSV line in the form: id,name,address,continent
     */
    public String toCSV() {
        return id + "," + name + "," + address + "," + continent;
    }

    /**
     * Deserializes a CSV line back into a Record.
     *
     * @param line a CSV string with exactly 4 comma-separated fields
     * @return the parsed Record
     * @throws IllegalArgumentException if the line is null, blank, or has wrong field count
     */
    public static Record fromCSV(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("CSV line must not be null or blank");
        }

        String[] parts = line.split(",", -1); // -1 preserves trailing empty strings
        if (parts.length != 4) {
            throw new IllegalArgumentException(
                    "Expected 4 fields but got " + parts.length + " in: " + truncate(line, 80));
        }

        int id;
        try {
            id = Integer.parseInt(parts[0].strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid id '" + parts[0].strip() + "' — must be a 32-bit integer", e);
        }

        String name = parts[1];
        String address = parts[2];
        String continent = parts[3];

        return new Record(id, name, address, continent);
    }

    /**
     * Rough estimate of in-memory size in bytes.
     * Useful for memory-budget calculations during external sorting.
     *
     * Breakdown: 4 bytes (int id) + 2 bytes per char for each String field
     *            + ~40 bytes object/header overhead.
     */
    public int estimatedSizeBytes() {
        return 4
                + name.length() * 2
                + address.length() * 2
                + continent.length() * 2
                + 40; // object header + record overhead + references
    }

    /**
     * Computes a 64-bit hash used for content integrity verification.
     * Two records with the same field values always produce the same hash.
     * The global XOR/sum of hashes over a set of records is order-independent,
     * so it can verify that sorting preserved all records without mutation.
     */
    public long hash64() {
        long h = id;
        h = h * 31L + name.hashCode();
        h = h * 31L + address.hashCode();
        h = h * 31L + continent.hashCode();
        return h;
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
