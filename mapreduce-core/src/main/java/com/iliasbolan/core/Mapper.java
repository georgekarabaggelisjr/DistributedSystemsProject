package com.iliasbolan.core;

/**
 * The core contract for the Map phase of a Map-Reduce job.
 * <p>
 * Users submitting a compute job must provide a compiled Java class that implements
 * this interface. The Worker node's execution engine will invoke the
 * {@link #map(String, Context)} method for each chunk of data.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-03-30
 */
public interface Mapper {

    /**
     * Processes a single input data chunk and emits intermediate key-value pairs
     * to the provided context.
     *
     * @param value   The raw input value (e.g., a block/chunk of text).
     * @param context The execution context for emitting intermediate records.
     */
    void map(String value, Context context);
}