package com.pipeline.sort;

import com.pipeline.model.Record;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RunMerger} — Phase B of external merge sort.
 */
class RunMergerTest {

    @TempDir
    Path tempDir;

    private static final Comparator<Record> BY_ID = Comparator.comparingInt(Record::id);

    /**
     * Single-pass merge: 5 pre-sorted files with fan-in 5. Output must be globally
     * sorted and contain the sum of all input records.
     */
    @Test
    void testSinglePassMerge() throws IOException {
        int fileCount = 5;
        int recordsPerFile = 200;

        List<Path> runFiles = createSortedFixtures(fileCount, recordsPerFile, 0);
        int expectedTotal = fileCount * recordsPerFile;

        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 5, tempDir);
        merger.merge(runFiles, output::add);

        assertEquals(expectedTotal, output.size(),
                "Merged output count must equal sum of all input file counts");
        assertGloballySorted(output, BY_ID);
    }

    /**
     * Multi-pass merge: 10 files with fan-in 3. Requires ceil(10/3) = 4 intermediate
     * files in the first pass, then ceil(4/3) = 2 in the second, then a final merge.
     */
    @Test
    void testMultiPassMerge() throws IOException {
        int fileCount = 10;
        int recordsPerFile = 150;

        List<Path> runFiles = createSortedFixtures(fileCount, recordsPerFile, 0);
        int expectedTotal = fileCount * recordsPerFile;

        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 3, tempDir);
        merger.merge(runFiles, output::add);

        assertEquals(expectedTotal, output.size(),
                "Multi-pass merge must preserve total record count");
        assertGloballySorted(output, BY_ID);
    }

    /**
     * Single run file: should stream through without errors.
     */
    @Test
    void testSingleRunFile() throws IOException {
        List<Path> runFiles = createSortedFixtures(1, 300, 0);

        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 16, tempDir);
        merger.merge(runFiles, output::add);

        assertEquals(300, output.size());
        assertGloballySorted(output, BY_ID);
    }

    /**
     * Empty run file: merge should complete gracefully with no output.
     */
    @Test
    void testEmptyRunFile() throws IOException {
        Path emptyFile = tempDir.resolve("empty_run.csv");
        Files.createFile(emptyFile);

        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 16, tempDir);
        merger.merge(List.of(emptyFile), output::add);

        assertTrue(output.isEmpty(), "Empty run file should produce no output");
    }

    /**
     * Null / empty list: merge should be a no-op.
     */
    @Test
    void testEmptyRunFileList() {
        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 16, tempDir);
        merger.merge(List.of(), output::add);

        assertTrue(output.isEmpty());
    }

    /**
     * Multi-pass with fan-in 2 (minimum): 8 files → 4 rounds of 2-way merges.
     */
    @Test
    void testMinimumFanIn() throws IOException {
        int fileCount = 8;
        int recordsPerFile = 100;

        List<Path> runFiles = createSortedFixtures(fileCount, recordsPerFile, 0);
        int expectedTotal = fileCount * recordsPerFile;

        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 2, tempDir);
        merger.merge(runFiles, output::add);

        assertEquals(expectedTotal, output.size());
        assertGloballySorted(output, BY_ID);
    }

    /**
     * Merge with duplicate IDs across files: must preserve all duplicates in order.
     */
    @Test
    void testMergeWithDuplicateIds() throws IOException {
        // Two files, each containing records with IDs 1..50 — all duplicates
        List<Path> runFiles = new ArrayList<>();
        runFiles.add(createSortedFile("dup_run_0.csv", 1, 50));
        runFiles.add(createSortedFile("dup_run_1.csv", 1, 50));

        List<Record> output = new ArrayList<>();
        RunMerger merger = new RunMerger(BY_ID, 16, tempDir);
        merger.merge(runFiles, output::add);

        assertEquals(100, output.size(), "All duplicates must be preserved");
        assertGloballySorted(output, BY_ID);
    }

    // ── Fixture Helpers ─────────────────────────────────────────────────────

    /**
     * Creates {@code fileCount} pre-sorted fixture files, each with
     * {@code recordsPerFile} records. IDs are interleaved across files to
     * produce a realistic merge workload.
     *
     * File k contains IDs: idOffset + k, idOffset + k + fileCount,
     *   idOffset + k + 2*fileCount, ...
     * This ensures each file is sorted and the global order requires merging.
     */
    private List<Path> createSortedFixtures(int fileCount, int recordsPerFile, int idOffset)
            throws IOException {
        List<Path> paths = new ArrayList<>();
        for (int f = 0; f < fileCount; f++) {
            Path file = tempDir.resolve(String.format("fixture_run_%04d.csv", f));
            try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                for (int r = 0; r < recordsPerFile; r++) {
                    int id = idOffset + f + r * fileCount; // interleaved, but sorted within each file
                    Record record = new Record(id,
                            "Name_" + id,
                            "Address_" + id,
                            "Continent_" + (id % 7));
                    if (r > 0) {
                        writer.newLine();
                    }
                    writer.write(record.toCSV());
                }
            }
            paths.add(file);
        }
        return paths;
    }

    /**
     * Creates a single sorted fixture file with IDs from {@code startId} to
     * {@code endId} (inclusive).
     */
    private Path createSortedFile(String fileName, int startId, int endId) throws IOException {
        Path file = tempDir.resolve(fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int id = startId; id <= endId; id++) {
                if (id > startId) {
                    writer.newLine();
                }
                Record record = new Record(id, "Name_" + id, "Addr_" + id, "C_" + (id % 5));
                writer.write(record.toCSV());
            }
        }
        return file;
    }

    private void assertGloballySorted(List<Record> records, Comparator<Record> cmp) {
        for (int i = 1; i < records.size(); i++) {
            assertTrue(cmp.compare(records.get(i - 1), records.get(i)) <= 0,
                    "Global sort violation at index " + i +
                            ": " + records.get(i - 1) + " > " + records.get(i));
        }
    }
}
