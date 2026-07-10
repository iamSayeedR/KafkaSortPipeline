package com.pipeline.sort;

import com.pipeline.model.Record;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Phase A of external merge sort — buffer, sort, spill to disk.
 *
 * <p>Records are accumulated in an in-memory buffer. When the estimated serialized
 * byte size of the buffer exceeds {@code bufferSizeBytes}, the buffer is sorted
 * using {@link Arrays#parallelSort} and flushed to a temporary run file as
 * newline-delimited CSV. After all records have been added, call {@link #finish()}
 * to flush any remaining buffer and retrieve the list of run file paths.</p>
 *
 * <p>For small datasets where all records fit in a single run, {@code finish()}
 * will force-split the buffer into two runs (provided there are more than 1000
 * records) so that the merge path is always exercised.</p>
 */
public class RunWriter {

    private final Comparator<Record> comparator;
    private final long bufferSizeBytes;
    private final Path tempDir;

    private final List<Record> buffer = new ArrayList<>();
    private long currentBufferBytes = 0;
    private int runCount = 0;
    private final List<Path> runFiles = new ArrayList<>();

    /**
     * @param comparator     ordering for in-memory sort of each run
     * @param bufferSizeBytes max estimated serialized bytes before spilling (default 150 MB = 157286400)
     * @param tempDir         directory to write run files into
     */
    public RunWriter(Comparator<Record> comparator, long bufferSizeBytes, Path tempDir) {
        this.comparator = Objects.requireNonNull(comparator, "comparator must not be null");
        if (bufferSizeBytes <= 0) {
            throw new IllegalArgumentException("bufferSizeBytes must be positive");
        }
        this.tempDir = Objects.requireNonNull(tempDir, "tempDir must not be null");
        this.bufferSizeBytes = bufferSizeBytes;
    }

    /**
     * Adds a record to the in-memory buffer. If the buffer's estimated byte size
     * reaches or exceeds the configured threshold, the buffer is sorted and spilled
     * to a run file on disk.
     *
     * @param record the record to add
     */
    public void add(Record record) {
        Objects.requireNonNull(record, "record must not be null");
        buffer.add(record);
        currentBufferBytes += record.estimatedSizeBytes();
        if (currentBufferBytes >= bufferSizeBytes) {
            spillRun(buffer);
            buffer.clear();
            currentBufferBytes = 0;
        }
    }

    /**
     * Flushes any remaining records in the buffer as a final run and returns
     * the list of all run file paths produced.
     *
     * <p>If only one run would result and the buffer contains more than 1000
     * records, the buffer is split in half and spilled as two separate runs.
     * This guarantees that the k-way merge path is exercised even with small
     * datasets.</p>
     *
     * @return unmodifiable list of paths to the sorted run files
     */
    public List<Path> finish() {
        if (!buffer.isEmpty()) {
            // If no runs have been spilled yet and there are enough records, force two runs
            if (runFiles.isEmpty() && buffer.size() > 1000) {
                int mid = buffer.size() / 2;
                List<Record> firstHalf = new ArrayList<>(buffer.subList(0, mid));
                List<Record> secondHalf = new ArrayList<>(buffer.subList(mid, buffer.size()));
                spillRun(firstHalf);
                spillRun(secondHalf);
            } else {
                spillRun(buffer);
            }
            buffer.clear();
            currentBufferBytes = 0;
        }
        return List.copyOf(runFiles);
    }

    /**
     * Sorts the current buffer using {@link Arrays#parallelSort} and writes it
     * to a run file as newline-delimited CSV.
     */
    private void spillRun(List<Record> runBuffer) {
        if (runBuffer.isEmpty()) {
            return;
        }

        Record[] arr = runBuffer.toArray(new Record[0]);
        Arrays.parallelSort(arr, comparator);

        Path runFile = tempDir.resolve(String.format("run_%04d.csv", runCount));
        try {
            Files.createDirectories(tempDir);
            try (BufferedWriter writer = Files.newBufferedWriter(runFile, StandardCharsets.UTF_8)) {
                for (int i = 0; i < arr.length; i++) {
                    if (i > 0) {
                        writer.newLine();
                    }
                    writer.write(arr[i].toCSV());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write run file: " + runFile, e);
        }

        runFiles.add(runFile);
        runCount++;
    }
}
