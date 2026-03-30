package com.iliasbolan.core;

/**
 * Represents a fundamental key-value data structure used throughout the Map-Reduce execution pipeline.
 * <p>
 * This Data Transfer Object (DTO) is utilized to encapsulate the intermediate
 * data generated during the Map phase, which is subsequently shuffled, sorted,
 * and consumed during the Reduce phase.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @since 2026-03-30
 */
public class KeyValuePair {

    /** The key associated with the intermediate or final data record. */
    public String key;

    /** The value associated with the intermediate or final data record. */
    public String value;

    /**
     * Constructs a new {@code KeyValuePair} with the specified key and value.
     *
     * @param key   The key of the record.
     * @param value The value of the record.
     */
    public KeyValuePair(String key, String value) {
        this.key = key;
        this.value = value;
    }
}