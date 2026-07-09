package com.pipeline.sort;

import com.pipeline.model.Record;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RunWriter} — Phase A of external merge sort.
 */
class RunWriterTest {

    @TempDir
    Path tempDir;

    private static final Comparator<Record> BY_ID = Comparator.comparingInt(Record::id);

    /**
     * With a 50 KB buffer and 10,000 records (each ~65 bytes), the writer should
     * produce multiple run files, each individually sorted by ID.
     */
    @Test
    void testMultipleRunsWithTinyBuffer() throws IOException {
        long tinyBuffer = 51_200L; // 50 KB
        RunWriter writer = new RunWriter(BY_ID, tinyBuffer, tempDir);

        Random rng = new Random(42);
        int recordCount = 10_000;

        for (int i = 0; i < recordCount; i++) {
            int id = rng.nextInt(1_000_000);
            Record record = new Record(id,
                    "Name_" + i,
                    "Address_" + i + " Street",
                    "Continent_" + (i % 7));
            writer.add(record);
        }

        List<Path> runFiles = writer.finish();

        // Must produce at least 2 run files
        assertTrue(runFiles.size() >= 2,
                "Expected at least 2 run files with 50KB buffer and 10K records, got " + runFiles.size());

        // Verify each run file is individually sorted by ID and count total records
        int totalRecords = 0;
        for (Path runFile : runFiles) {
            List<Record> records = readRunFile(runFile);
            assertFalse(records.isEmpty(), "Run file should not be empty: " + runFile);
            totalRecords += records.size();
            assertSorted(records, BY_ID, "Run file " + runFile.getFileName() + " is not sorted by ID");
        }

        // Total records across all runs must equal input count
        assertEquals(recordCount, totalRecords,
                "Total records across all runs must equal input count");
    }

    /**
     * With a 150 MB buffer and only 5,000 records, the force-split mechanism should
     * kick in and produce at least 2 run files.
     */
    @Test
    void testForceSplitWithLargeBuffer() throws IOException {
        long largeBuffer = 157_286_400L; // 150 MB
        RunWriter writer = new RunWriter(BY_ID, largeBuffer, tempDir);

        Random rng = new Random(99);
        int recordCount = 5_000;

        for (int i = 0; i < recordCount; i++) {
            int id = rng.nextInt(500_000);
            Record record = new Record(id,
                    "Person_" + i,
                    "Addr_" + i,
                    "Continent_" + (i % 5));
            writer.add(record);
        }

        List<Path> runFiles = writer.finish();

        // Force-split should produce at least 2 runs
        assertTrue(runFiles.size() >= 2,
                "Expected force-split to produce at least 2 runs, got " + runFiles.size());

        // All runs must be sorted
        int totalRecords = 0;
        for (Path runFile : runFiles) {
            List<Record> records = readRunFile(runFile);
            totalRecords += records.size();
            assertSorted(records, BY_ID, "Force-split run " + runFile.getFileName() + " is not sorted");
        }

        assertEquals(recordCount, totalRecords,
                "Force-split: total records must equal input count");
    }

    /**
     * With a small number of records (<=1000) and a large buffer, force-split
     * should NOT activate — only 1 run file should be produced.
     */
    @Test
    void testSmallDatasetNoForceSplit() throws IOException {
        long largeBuffer = 157_286_400L;
        RunWriter writer = new RunWriter(BY_ID, largeBuffer, tempDir);

        for (int i = 0; i < 500; i++) {
            writer.add(new Record(500 - i, "N_" + i, "A_" + i, "C_" + (i % 3)));
        }

        List<Path> runFiles = writer.finish();

        assertEquals(1, runFiles.size(),
                "With <= 1000 records and large buffer, should produce exactly 1 run");

        List<Record> records = readRunFile(runFiles.get(0));
        assertEquals(500, records.size());
        assertSorted(records, BY_ID, "Single run must be sorted");
    }

    /**
     * Verifies run files are named with zero-padded numbers.
     */
    @Test
    void testRunFileNaming() throws IOException {
        long tinyBuffer = 200L; // Very tiny to force many runs
        RunWriter writer = new RunWriter(BY_ID, tinyBuffer, tempDir);

        for (int i = 0; i < 100; i++) {
            writer.add(new Record(i, "N", "A", "C"));
        }

        List<Path> runFiles = writer.finish();
        assertTrue(runFiles.size() >= 2, "Should produce multiple runs");

        for (int i = 0; i < runFiles.size(); i++) {
            String expectedName = String.format("run_%04d.csv", i);
            assertEquals(expectedName, runFiles.get(i).getFileName().toString(),
                    "Run file " + i + " should have zero-padded name");
        }
    }

    /**
     * Empty input produces no run files.
     */
    @Test
    void testEmptyInput() {
        RunWriter writer = new RunWriter(BY_ID, 51_200L, tempDir);
        List<Path> runFiles = writer.finish();
        assertTrue(runFiles.isEmpty(), "No records added, no run files expected");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private List<Record> readRunFile(Path file) throws IOException {
        List<Record> records = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    records.add(Record.fromCSV(line));
                }
            }
        }
        return records;
    }

    private void assertSorted(List<Record> records, Comparator<Record> cmp, String message) {
        for (int i = 1; i < records.size(); i++) {
            assertTrue(cmp.compare(records.get(i - 1), records.get(i)) <= 0,
                    message + " — violation at index " + i +
                            ": " + records.get(i - 1) + " > " + records.get(i));
        }
    }
}
