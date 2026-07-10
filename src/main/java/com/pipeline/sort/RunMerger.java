package com.pipeline.sort;

import com.pipeline.model.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Consumer;

/**
 * Phase B of external merge sort — bounded-fan-in k-way merge.
 *
 * <p>Merges a list of pre-sorted run files into a single globally sorted stream.
 * If the number of run files exceeds {@code maxFanIn}, multi-pass merging is
 * performed: runs are grouped into batches, each batch is merged into an
 * intermediate file, and the process repeats until the number of files fits
 * within a single merge pass.</p>
 *
 * <p>Each input file is read through a {@link BufferedReader} with a 4 MB buffer.
 * A {@link PriorityQueue} drives the k-way merge, always extracting the smallest
 * record across all open sources.</p>
 */
public class RunMerger {

    private static final int READ_BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB

    private final Comparator<Record> comparator;
    private final int maxFanIn;
    private final Path tempDir;
    private int intermediateCount = 0;

    /**
     * @param comparator merge order
     * @param maxFanIn   max number of files to merge simultaneously (default 16)
     * @param tempDir    directory for intermediate merge files
     */
    public RunMerger(Comparator<Record> comparator, int maxFanIn, Path tempDir) {
        this.comparator = Objects.requireNonNull(comparator, "comparator must not be null");
        if (maxFanIn < 2) {
            throw new IllegalArgumentException("maxFanIn must be at least 2");
        }
        this.tempDir = Objects.requireNonNull(tempDir, "tempDir must not be null");
        this.maxFanIn = maxFanIn;
    }

    /**
     * Merges the given run files and streams the globally sorted output to the
     * provided consumer.
     *
     * <p>If the number of run files is within {@code maxFanIn}, a single-pass
     * merge is performed directly to the output consumer. Otherwise, multi-pass
     * merging reduces the file count in rounds until a final single-pass merge
     * can complete.</p>
     *
     * @param runFiles list of paths to pre-sorted run files
     * @param output   consumer that receives each record in globally sorted order
     */
    public void merge(List<Path> runFiles, Consumer<Record> output) {
        if (runFiles == null || runFiles.isEmpty()) {
            return;
        }

        List<Path> allTempFiles = new ArrayList<>(runFiles);
        List<Path> currentFiles = new ArrayList<>(runFiles);

        // Multi-pass reduction
        while (currentFiles.size() > maxFanIn) {
            List<Path> nextRound = mergeToFiles(currentFiles);
            allTempFiles.addAll(nextRound);
            currentFiles = nextRound;
        }

        // Final merge pass → output consumer
        mergePass(currentFiles, output);

        // Cleanup all run and intermediate files
        for (Path file : allTempFiles) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                // Best-effort cleanup; log but do not fail
                System.err.println("Warning: could not delete temp file " + file + ": " + e.getMessage());
            }
        }
    }

    /**
     * Performs the actual k-way merge of the given input files, streaming each
     * record in sorted order to the output consumer.
     */
    private void mergePass(List<Path> inputs, Consumer<Record> output) {
        if (inputs.isEmpty()) {
            return;
        }

        List<BufferedReader> readers = new ArrayList<>(inputs.size());
        try {
            // Open all readers
            for (Path input : inputs) {
                readers.add(new BufferedReader(
                        new FileReader(input.toFile(), StandardCharsets.UTF_8),
                        READ_BUFFER_SIZE
                ));
            }

            PriorityQueue<IndexedRecord> heap = new PriorityQueue<>(
                    (a, b) -> comparator.compare(a.record(), b.record())
            );

            // Seed the heap with the first record from each file
            for (int i = 0; i < readers.size(); i++) {
                String line = readers.get(i).readLine();
                if (line != null && !line.isEmpty()) {
                    heap.offer(new IndexedRecord(Record.fromCSV(line), i));
                }
            }

            // Main merge loop
            while (!heap.isEmpty()) {
                IndexedRecord min = heap.poll();
                output.accept(min.record());

                String nextLine = readers.get(min.sourceIndex()).readLine();
                if (nextLine != null && !nextLine.isEmpty()) {
                    heap.offer(new IndexedRecord(Record.fromCSV(nextLine), min.sourceIndex()));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error during k-way merge", e);
        } finally {
            // Close all readers
            for (BufferedReader reader : readers) {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        System.err.println("Warning: failed to close reader: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Groups the input files into batches of {@code maxFanIn} and merges each
     * batch into an intermediate file. Returns the list of intermediate file paths.
     */
    private List<Path> mergeToFiles(List<Path> inputs) {
        List<Path> intermediateFiles = new ArrayList<>();

        for (int start = 0; start < inputs.size(); start += maxFanIn) {
            int end = Math.min(start + maxFanIn, inputs.size());
            List<Path> batch = inputs.subList(start, end);

            Path intermediateFile = tempDir.resolve(
                    String.format("intermediate_%04d.csv", intermediateCount++)
            );

            try {
                Files.createDirectories(tempDir);
                try (BufferedWriter writer = Files.newBufferedWriter(intermediateFile, StandardCharsets.UTF_8)) {
                    final boolean[] first = {true};
                    mergePass(batch, record -> {
                        try {
                            if (!first[0]) {
                                writer.newLine();
                            }
                            writer.write(record.toCSV());
                            first[0] = false;
                        } catch (IOException e) {
                            throw new UncheckedIOException("Failed to write intermediate file", e);
                        }
                    });
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create intermediate merge file: " + intermediateFile, e);
            }

            intermediateFiles.add(intermediateFile);
        }

        return intermediateFiles;
    }

    /**
     * Holds a record together with the index of the source file it came from,
     * enabling the priority queue to refill from the correct reader.
     */
    private record IndexedRecord(Record record, int sourceIndex) {}
}
