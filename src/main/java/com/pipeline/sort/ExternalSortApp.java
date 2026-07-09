package com.pipeline.sort;

import com.pipeline.kafka.ConsumerFactory;
import com.pipeline.kafka.ProducerFactory;
import com.pipeline.model.Record;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * Orchestrates Phase A (run generation) → Phase B (k-way merge) → produce to
 * output Kafka topic.
 *
 * <p>Usage: {@code java ExternalSortApp <SORT_KEY>} where SORT_KEY is one of
 * ID, NAME, CONTINENT.</p>
 *
 * <p>Configuration via environment variables:</p>
 * <ul>
 *   <li>{@code RUN_BUFFER_BYTES} — Phase A buffer threshold in bytes (default 157286400 = 150 MB)</li>
 *   <li>{@code MAX_FAN_IN} — Phase B max merge fan-in (default 16)</li>
 * </ul>
 */
public class ExternalSortApp {

    private static final long DEFAULT_BUFFER_BYTES = 157_286_400L; // 150 MB
    private static final int DEFAULT_MAX_FAN_IN = 16;
    private static final String SOURCE_TOPIC = "source";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final int MAX_EMPTY_POLLS = 10;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: ExternalSortApp <SORT_KEY>");
            System.err.println("  SORT_KEY: ID | NAME | CONTINENT");
            System.exit(1);
        }

        SortKey sortKey;
        try {
            sortKey = SortKey.valueOf(args[0].toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid sort key: " + args[0] + ". Must be one of: ID, NAME, CONTINENT");
            System.exit(1);
            return; // unreachable, but keeps compiler happy
        }

        long bufferSizeBytes = parseLongEnv("RUN_BUFFER_BYTES", DEFAULT_BUFFER_BYTES);
        int maxFanIn = parseIntEnv("MAX_FAN_IN", DEFAULT_MAX_FAN_IN);
        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), "sort-runs-" + sortKey.name());

        System.out.printf("Starting external sort: key=%s, buffer=%d bytes, fanIn=%d, tempDir=%s%n",
                sortKey, bufferSizeBytes, maxFanIn, tempDir);

        long totalStart = System.currentTimeMillis();

        // ── Phase A: Run Generation ──────────────────────────────────────────
        long phaseAStart = System.currentTimeMillis();
        long recordsConsumed = 0;
        List<Path> runFiles;

        RunWriter runWriter = new RunWriter(sortKey.comparator(), bufferSizeBytes, tempDir);

        try (KafkaConsumer<String, String> consumer = ConsumerFactory.create("sort-" + sortKey.topicName())) {
            consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));

            int emptyPollCount = 0;
            long startWaitMs = System.currentTimeMillis();
            long maxWaitMs = 60000; // Wait up to 60s for first record

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    if (recordsConsumed > 0) {
                        emptyPollCount++;
                        if (emptyPollCount >= MAX_EMPTY_POLLS) {
                            break; // topic exhausted
                        }
                    } else {
                        if (System.currentTimeMillis() - startWaitMs > maxWaitMs) {
                            System.out.printf("Timed out waiting for first record from topic '%s' after 60s.%n", SOURCE_TOPIC);
                            break;
                        }
                    }
                    continue;
                }
                emptyPollCount = 0;
                for (ConsumerRecord<String, String> cr : records) {
                    Record record = Record.fromCSV(cr.value());
                    runWriter.add(record);
                    recordsConsumed++;
                }
            }
        }

        runFiles = runWriter.finish();
        long phaseAMs = System.currentTimeMillis() - phaseAStart;

        System.out.printf("Phase A complete: %d runs generated from %d records%n",
                runFiles.size(), recordsConsumed);
        System.out.printf("TIMING:Sort_%s_RunGen:%dms%n", sortKey.name(), phaseAMs);

        // ── Phase B: K-way Merge + Produce ──────────────────────────────────
        long phaseBStart = System.currentTimeMillis();
        long[] mergedCount = {0};

        RunMerger merger = new RunMerger(sortKey.comparator(), maxFanIn, tempDir);

        try (KafkaProducer<String, String> producer = ProducerFactory.create()) {
            String outputTopic = sortKey.topicName();
            merger.merge(runFiles, record -> {
                producer.send(new ProducerRecord<>(outputTopic, null, record.toCSV()));
                mergedCount[0]++;
            });
            producer.flush();
        }

        long phaseBMs = System.currentTimeMillis() - phaseBStart;

        System.out.printf("Phase B complete: %d records merged and produced to topic %s%n",
                mergedCount[0], sortKey.topicName());
        System.out.printf("TIMING:Sort_%s_Merge:%dms%n", sortKey.name(), phaseBMs);

        long totalMs = System.currentTimeMillis() - totalStart;
        System.out.printf("TIMING:Sort_%s_Total:%dms%n", sortKey.name(), totalMs);
    }

    private static long parseLongEnv(String envVar, long defaultValue) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed <= 0) {
                System.err.printf("Warning: %s=%s is not positive, using default %d%n",
                        envVar, value, defaultValue);
                return defaultValue;
            }
            return parsed;
        } catch (NumberFormatException e) {
            System.err.printf("Warning: %s=%s is not a valid number, using default %d%n",
                    envVar, value, defaultValue);
            return defaultValue;
        }
    }

    private static int parseIntEnv(String envVar, int defaultValue) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed <= 0) {
                System.err.printf("Warning: %s=%s is not positive, using default %d%n",
                        envVar, value, defaultValue);
                return defaultValue;
            }
            return parsed;
        } catch (NumberFormatException e) {
            System.err.printf("Warning: %s=%s is not a valid number, using default %d%n",
                    envVar, value, defaultValue);
            return defaultValue;
        }
    }
}
