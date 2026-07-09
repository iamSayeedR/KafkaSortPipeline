package com.pipeline.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RecordTest {

    // ── Round-trip tests ────────────────────────────────────────────────

    @Test
    void roundTrip_typicalRecord() {
        var original = new Record(42, "Alice", "123 Main St", "Europe");
        var roundTripped = Record.fromCSV(original.toCSV());
        assertEquals(original, roundTripped);
    }

    @Test
    void roundTrip_preservesAllFields() {
        var original = new Record(99, "Bob", "456 Oak Ave", "Asia");
        String csv = original.toCSV();
        assertEquals("99,Bob,456 Oak Ave,Asia", csv);
        assertEquals(original, Record.fromCSV(csv));
    }

    // ── Edge-case IDs ───────────────────────────────────────────────────

    @Test
    void roundTrip_negativeId() {
        var original = new Record(-7, "Neg", "addr", "Africa");
        assertEquals(original, Record.fromCSV(original.toCSV()));
    }

    @Test
    void roundTrip_intMinValue() {
        var original = new Record(Integer.MIN_VALUE, "MinVal", "somewhere", "Australia");
        var csv = original.toCSV();
        assertTrue(csv.startsWith(Integer.MIN_VALUE + ","));
        assertEquals(original, Record.fromCSV(csv));
    }

    @Test
    void roundTrip_intMaxValue() {
        var original = new Record(Integer.MAX_VALUE, "MaxVal", "elsewhere", "South America");
        assertEquals(original, Record.fromCSV(original.toCSV()));
    }

    @Test
    void roundTrip_zeroId() {
        var original = new Record(0, "Zero", "nowhere", "North America");
        assertEquals(original, Record.fromCSV(original.toCSV()));
    }

    // ── fromCSV error handling ──────────────────────────────────────────

    @Test
    void fromCSV_nullInput_throws() {
        assertThrows(IllegalArgumentException.class, () -> Record.fromCSV(null));
    }

    @Test
    void fromCSV_blankInput_throws() {
        assertThrows(IllegalArgumentException.class, () -> Record.fromCSV("   "));
    }

    @Test
    void fromCSV_emptyInput_throws() {
        assertThrows(IllegalArgumentException.class, () -> Record.fromCSV(""));
    }

    @Test
    void fromCSV_tooFewFields_throws() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> Record.fromCSV("1,Alice,somewhere"));
        assertTrue(ex.getMessage().contains("Expected 4 fields"));
    }

    @Test
    void fromCSV_tooManyFields_throws() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> Record.fromCSV("1,Alice,somewhere,Europe,extra"));
        assertTrue(ex.getMessage().contains("Expected 4 fields"));
    }

    @Test
    void fromCSV_nonNumericId_throws() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> Record.fromCSV("abc,Alice,somewhere,Europe"));
        assertTrue(ex.getMessage().contains("Invalid id"));
    }

    @Test
    void fromCSV_emptyId_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> Record.fromCSV(",Alice,somewhere,Europe"));
    }

    // ── Null field rejection ────────────────────────────────────────────

    @Test
    void constructor_nullName_throws() {
        assertThrows(NullPointerException.class,
                () -> new Record(1, null, "addr", "Europe"));
    }

    @Test
    void constructor_nullAddress_throws() {
        assertThrows(NullPointerException.class,
                () -> new Record(1, "name", null, "Europe"));
    }

    @Test
    void constructor_nullContinent_throws() {
        assertThrows(NullPointerException.class,
                () -> new Record(1, "name", "addr", null));
    }

    // ── hash64 consistency ──────────────────────────────────────────────

    @Test
    void hash64_sameRecord_sameHash() {
        var r1 = new Record(42, "Alice", "123 Main St", "Europe");
        var r2 = new Record(42, "Alice", "123 Main St", "Europe");
        assertEquals(r1.hash64(), r2.hash64());
    }

    @Test
    void hash64_consistent_acrossMultipleCalls() {
        var record = new Record(-999, "Test", "Addr", "Asia");
        long first = record.hash64();
        for (int i = 0; i < 100; i++) {
            assertEquals(first, record.hash64(), "hash64 must be deterministic");
        }
    }

    @Test
    void hash64_differentRecords_likelyDifferent() {
        var r1 = new Record(1, "Alice", "addr1", "Europe");
        var r2 = new Record(2, "Bob", "addr2", "Asia");
        // Not guaranteed, but extremely unlikely to collide for distinct inputs
        assertNotEquals(r1.hash64(), r2.hash64());
    }

    @Test
    void hash64_roundTrip_preserved() {
        var original = new Record(42, "Alice", "123 Main St", "Europe");
        var parsed = Record.fromCSV(original.toCSV());
        assertEquals(original.hash64(), parsed.hash64(),
                "hash64 must survive CSV round-trip");
    }

    // ── estimatedSizeBytes ──────────────────────────────────────────────

    @Test
    void estimatedSizeBytes_positive() {
        var record = new Record(1, "Alice", "123 Main St", "Europe");
        assertTrue(record.estimatedSizeBytes() > 0);
    }

    @Test
    void estimatedSizeBytes_growsWithContent() {
        var small = new Record(1, "A", "B", "Asia");
        var large = new Record(1, "AliceBobCharlie", "123 Main Street Apt 42", "North America");
        assertTrue(large.estimatedSizeBytes() > small.estimatedSizeBytes());
    }
}
