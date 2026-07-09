package com.pipeline.generate;

import com.pipeline.kafka.ProducerFactory;
import com.pipeline.model.Record;
import com.pipeline.util.Args;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLI application that generates random records and streams them
 * to the "source" Kafka topic.
 *
 * <p>Usage: {@code java -cp ... com.pipeline.generate.DataGeneratorApp [recordCount]}</p>
 *
 * <p>Record count defaults to the RECORD_COUNT env var, or 50 000 000.</p>
 */
public class DataGeneratorApp {

    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorApp.class);
    private static final String TOPIC = "source";
    private static final int PROGRESS_INTERVAL = 1_000_000;

    public static void main(String[] args) {
        // Resolve record count: CLI arg > env var > default
        long recordCount = Args.getLong(args, 0,
                Long.parseLong(Args.env("RECORD_COUNT", "50000000")));

        LOG.info("Starting data generation: {} records → topic '{}'", recordCount, TOPIC);
        System.out.printf("Generating %,d records into topic '%s'%n", recordCount, TOPIC);

        var factory = new RandomRecordFactory();
        long startMs = System.currentTimeMillis();

        try (var producer = ProducerFactory.create()) {
            for (long i = 1; i <= recordCount; i++) {
                Record record = factory.nextRecord();
                producer.send(new ProducerRecord<>(
                        TOPIC,
                        String.valueOf(record.id()),  // key = string representation of id
                        record.toCSV()                // value = CSV line
                ));

                if (i % PROGRESS_INTERVAL == 0) {
                    System.out.printf("Generated %,d / %,d records...%n", i, recordCount);
                }
            }

            // Ensure all buffered records are sent before closing
            producer.flush();
        }

        long elapsedMs = System.currentTimeMillis() - startMs;
        System.out.printf("Data generation complete: %,d records in %,.1f s%n",
                recordCount, elapsedMs / 1000.0);
        System.out.printf("TIMING:DataGeneration:%dms%n", elapsedMs);
    }
}
