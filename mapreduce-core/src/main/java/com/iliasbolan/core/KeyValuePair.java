package com.iliasbolan.core;

/**
 * Represents a fundamental, immutable key-value data structure used throughout the Map-Reduce execution pipeline.
 * <p>
 * <b>Architectural Role:</b><br>
 * This Data Transfer Object (DTO) encapsulates the intermediate data generated during
 * the Map phase, which is subsequently shuffled, sorted, and consumed during the Reduce phase.
 * </p>
 * <p>
 * <b>Immutability & Hashing:</b><br>
 * By utilizing a Java 17 <code>record</code>, this object is strictly immutable, guaranteeing
 * thread-safety during highly parallel Fork/Join operations. Furthermore, the JVM automatically
 * generates robust <code>equals()</code> and <code>hashCode()</code> methods, which are absolutely
 * critical for the Shuffle phase to reliably route identical keys to the same partition.
 * </p>
 *
 * @param key   The key associated with the intermediate or final data record.
 * @param value The value associated with the intermediate or final data record.
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-03-30
 */
public record KeyValuePair(String key, String value) {}