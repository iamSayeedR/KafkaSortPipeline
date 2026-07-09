package com.pipeline.kafka;

import com.pipeline.util.Args;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Creates pre-configured {@link KafkaProducer} instances tuned for
 * high-throughput CSV record ingestion.
 */
public class ProducerFactory {

    private ProducerFactory() {} // utility class

    /**
     * Creates a producer using bootstrap servers from the
     * KAFKA_BOOTSTRAP_SERVERS environment variable (default: localhost:9092).
     */
    public static KafkaProducer<String, String> create() {
        return create(Args.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
    }

    /**
     * Creates a producer targeting the given bootstrap servers.
     *
     * @param bootstrapServers comma-separated list of host:port
     * @return a new KafkaProducer — caller must close via try-with-resources
     */
    public static KafkaProducer<String, String> create(String bootstrapServers) {
        var props = new Properties();

        // ── Connection ──────────────────────────────────────────────────
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ── Serialization ───────────────────────────────────────────────
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // ── Durability ──────────────────────────────────────────────────
        // acks=1: leader acknowledges; good balance of speed vs safety.
        // For this pipeline, data can be regenerated, so we prefer throughput.
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // ── Batching ────────────────────────────────────────────────────
        // linger.ms=15: wait up to 15ms to fill a batch before sending.
        // Improves throughput by coalescing small sends into fewer requests.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 15);

        // batch.size=128KB: maximum batch size in bytes for a single partition.
        // Larger batches compress better and reduce request overhead.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);

        // ── Compression ─────────────────────────────────────────────────
        // zstd: excellent compression ratio with acceptable CPU cost.
        // Reduces network bandwidth and broker storage significantly.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // ── Memory ──────────────────────────────────────────────────────
        // buffer.memory=32MB: total memory the producer can use for buffering.
        // Must be large enough to hold in-flight batches for all partitions.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L);

        return new KafkaProducer<>(props);
    }
}
