package com.pipeline.kafka;

import com.pipeline.util.Args;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Creates pre-configured {@link KafkaConsumer} instances for reading
 * CSV records from Kafka topics.
 *
 * <p>Each sort job should use a unique {@code groupId} so that it can
 * independently consume the full source topic without interfering with
 * other consumers.</p>
 */
public class ConsumerFactory {

    private ConsumerFactory() {} // utility class

    /**
     * Creates a consumer using bootstrap servers from the
     * KAFKA_BOOTSTRAP_SERVERS environment variable (default: localhost:9092).
     *
     * @param groupId consumer group ID — use a unique value per sort job
     */
    public static KafkaConsumer<String, String> create(String groupId) {
        return create(groupId, Args.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
    }

    /**
     * Creates a consumer targeting the given bootstrap servers.
     *
     * @param groupId          consumer group ID
     * @param bootstrapServers comma-separated list of host:port
     * @return a new KafkaConsumer — caller must close via try-with-resources
     */
    public static KafkaConsumer<String, String> create(String groupId, String bootstrapServers) {
        var props = new Properties();

        // ── Connection ──────────────────────────────────────────────────
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ── Group ───────────────────────────────────────────────────────
        // Each sort job gets its own group ID so multiple consumers can
        // independently read the entire source topic from the beginning.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // ── Deserialization ─────────────────────────────────────────────
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // ── Fetch tuning ────────────────────────────────────────────────
        // fetch.min.bytes=64KB: broker waits until at least 64KB is available
        // before responding. Reduces the number of fetch requests for better throughput.
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536);

        // max.partition.fetch.bytes=1MB: maximum data returned per partition per fetch.
        // Balances memory usage vs throughput.
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);

        // ── Offset management ───────────────────────────────────────────
        // Manual commit — we control exactly when offsets are committed,
        // which is essential for at-least-once processing guarantees.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Start from the earliest offset when no committed offset exists.
        // This ensures new consumer groups read the full topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
}
