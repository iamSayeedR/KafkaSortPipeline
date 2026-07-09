package com.pipeline.verify;

import com.pipeline.kafka.ConsumerFactory;
import com.pipeline.model.Record;
import com.pipeline.sort.SortKey;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Streaming verification application that validates the pipeline output.
 *
 * <p>Performs three O(1)-memory checks:</p>
 * <ol>
 *   <li><b>Count</b> — all topics must contain the same number of records</li>
 *   <li><b>Sortedness</b> — each output topic must be sorted by its key</li>
 *   <li><b>Content integrity</b> — the 64-bit hash sum must match across all topics</li>
 * </ol>
 *
 * <p>Usage: {@code java -cp ... com.pipeline.verify.VerifyApp}</p>
 */
public class VerifyApp {

    private static final Logger LOG = LoggerFactory.getLogger(VerifyApp.class);

    /** All topics to verify: source + one per SortKey */
    private static final List<String> ALL_TOPICS = List.of(
            "source",
            SortKey.ID.topicName(),
            SortKey.NAME.topicName(),
            SortKey.CONTINENT.topicName()
    );

    /** Sorted output topics and their expected comparators */
    private static final Map<String, Comparator<Record>> SORTED_TOPICS = Map.of(
            SortKey.ID.topicName(), SortKey.ID.comparator(),
            SortKey.NAME.topicName(), SortKey.NAME.comparator(),
            SortKey.CONTINENT.topicName(), SortKey.CONTINENT.comparator()
    );

    /** Timeout for polling — if no records arrive within this window, we assume the topic is exhausted */
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    /** How many empty polls in a row before we consider consumption complete */
    private static final int MAX_EMPTY_POLLS = 3;

    public static void main(String[] args) {
        long startMs = System.currentTimeMillis();
        boolean allPassed = true;

        // ── Check 1: Counts ─────────────────────────────────────────────
        System.out.println("\n=== Check 1: Record Counts ===");
        Map<String, Long> counts = new LinkedHashMap<>();
        Map<String, Long> checksums = new LinkedHashMap<>();

        for (String topic : ALL_TOPICS) {
            long[] result = countAndChecksum(topic);
            counts.put(topic, result[0]);
            checksums.put(topic, result[1]);
        }

        boolean countPass = counts.values().stream().distinct().count() == 1;
        StringBuilder countMsg = new StringBuilder("COUNT CHECK:");
        for (var entry : counts.entrySet()) {
            countMsg.append(" ").append(entry.getKey()).append("=").append(entry.getValue()).append(",");
        }
        countMsg.setLength(countMsg.length() - 1); // remove trailing comma
        countMsg.append(" — ").append(countPass ? "PASS" : "FAIL");
        System.out.println(countMsg);
        if (!countPass) allPassed = false;

        // ── Check 2: Sortedness ─────────────────────────────────────────
        System.out.println("\n=== Check 2: Sort Order ===");
        for (var entry : SORTED_TOPICS.entrySet()) {
            boolean sortPass = verifySorted(entry.getKey(), entry.getValue());
            System.out.printf("SORT CHECK [%s]: %s%n", entry.getKey(), sortPass ? "PASS" : "FAIL");
            if (!sortPass) allPassed = false;
        }

        // ── Check 3: Content Integrity ──────────────────────────────────
        System.out.println("\n=== Check 3: Content Integrity (hash64 sum) ===");
        long sourceChecksum = checksums.get("source");
        boolean integrityPass = true;
        for (String topic : SORTED_TOPICS.keySet()) {
            if (checksums.get(topic) != sourceChecksum) {
                integrityPass = false;
            }
        }
        StringBuilder intMsg = new StringBuilder("INTEGRITY CHECK:");
        for (var entry : checksums.entrySet()) {
            intMsg.append(" ").append(entry.getKey()).append("=").append(entry.getValue()).append(",");
        }
        intMsg.setLength(intMsg.length() - 1);
        intMsg.append(" — ").append(integrityPass ? "PASS" : "FAIL");
        System.out.println(intMsg);
        if (!integrityPass) allPassed = false;

        // ── Summary ─────────────────────────────────────────────────────
        System.out.println();
        if (allPassed) {
            System.out.println("VERIFICATION: ALL PASSED");
        } else {
            System.out.println("VERIFICATION: FAILED (details above)");
        }

        long elapsedMs = System.currentTimeMillis() - startMs;
        System.out.printf("TIMING:Verification:%dms%n", elapsedMs);
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /**
     * Consumes the entire topic and returns [count, hash64Sum].
     * Uses a unique group ID so it always reads from the beginning.
     */
    private static long[] countAndChecksum(String topic) {
        String groupId = "verify-count-" + topic + "-" + UUID.randomUUID();
        long count = 0;
        long hashSum = 0;

        try (KafkaConsumer<String, String> consumer = ConsumerFactory.create(groupId)) {
            consumer.subscribe(List.of(topic));

            int emptyPolls = 0;
            long startWaitMs = System.currentTimeMillis();
            long maxWaitMs = 60000; // Wait up to 60s for the first record to arrive

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    if (count > 0) {
                        emptyPolls++;
                        if (emptyPolls >= MAX_EMPTY_POLLS) {
                            break; // topic exhausted
                        }
                    } else {
                        // Still waiting for first record (group rebalance / connection lag)
                        if (System.currentTimeMillis() - startWaitMs > maxWaitMs) {
                            LOG.info("Timed out waiting for first record on topic '{}' after 60s. Assuming empty.", topic);
                            break;
                        }
                    }
                    continue;
                }
                emptyPolls = 0;

                for (var cr : records) {
                    Record rec = Record.fromCSV(cr.value());
                    count++;
                    hashSum += rec.hash64(); // wrapping addition
                }
            }
        }

        LOG.info("Topic '{}': count={}, checksum={}", topic, count, hashSum);
        return new long[]{count, hashSum};
    }

    /**
     * Consumes the entire topic and verifies that records are in sorted order
     * according to the given comparator.
     */
    private static boolean verifySorted(String topic, Comparator<Record> comparator) {
        String groupId = "verify-sort-" + topic + "-" + UUID.randomUUID();
        Record previous = null;
        long position = 0;

        try (KafkaConsumer<String, String> consumer = ConsumerFactory.create(groupId)) {
            consumer.subscribe(List.of(topic));

            int emptyPolls = 0;
            long startWaitMs = System.currentTimeMillis();
            long maxWaitMs = 60000; // Wait up to 60s for first record

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    if (position > 0) {
                        emptyPolls++;
                        if (emptyPolls >= MAX_EMPTY_POLLS) {
                            break; // topic exhausted
                        }
                    } else {
                        if (System.currentTimeMillis() - startWaitMs > maxWaitMs) {
                            LOG.info("Timed out waiting for first record on sorted topic '{}' after 60s. Assuming empty.", topic);
                            break;
                        }
                    }
                    continue;
                }
                emptyPolls = 0;

                for (var cr : records) {
                    Record current = Record.fromCSV(cr.value());
                    position++;
                    if (previous != null && comparator.compare(previous, current) > 0) {
                        System.out.printf(
                                "  SORT VIOLATION at position %d in topic '%s':%n" +
                                "    prev: %s%n" +
                                "    curr: %s%n",
                                position, topic, previous.toCSV(), current.toCSV());
                        return false;
                    }
                    previous = current;
                }
            }
        }

        return true;
    }
}
