package com.pipeline.sort;

import com.pipeline.model.Record;

import java.util.Comparator;

/**
 * Defines the available sort keys for the pipeline.
 * Each value carries its own comparator and target topic name.
 */
public enum SortKey {

    ID(Comparator.comparingInt(Record::id), "id"),
    NAME(Comparator.comparing(Record::name), "name"),
    CONTINENT(Comparator.comparing(Record::continent), "continent");

    private final Comparator<Record> comparator;
    private final String topicName;

    SortKey(Comparator<Record> comparator, String topicName) {
        this.comparator = comparator;
        this.topicName = topicName;
    }

    /**
     * Returns the comparator that defines the sort order for this key.
     */
    public Comparator<Record> comparator() {
        return comparator;
    }

    /**
     * Returns the Kafka topic name where sorted output is written.
     */
    public String topicName() {
        return topicName;
    }

    /**
     * Resolves a SortKey from a case-insensitive string.
     *
     * @param s the string to match (e.g. "id", "Name", "CONTINENT")
     * @return the matching SortKey
     * @throws IllegalArgumentException if no match is found
     */
    public static SortKey fromString(String s) {
        if (s == null || s.isBlank()) {
            throw new IllegalArgumentException("Sort key must not be null or blank");
        }
        return switch (s.strip().toUpperCase()) {
            case "ID" -> ID;
            case "NAME" -> NAME;
            case "CONTINENT" -> CONTINENT;
            default -> throw new IllegalArgumentException(
                    "Unknown sort key '" + s + "'. Valid values: ID, NAME, CONTINENT");
        };
    }
}
