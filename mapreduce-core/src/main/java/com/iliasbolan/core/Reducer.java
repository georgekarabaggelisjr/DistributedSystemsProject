package com.iliasbolan.core;

import java.util.Iterator;

/**
 * The core contract for the Reduce phase of a Map-Reduce job.
 * <p>
 * Users submitting a compute job must provide a compiled Java class that implements
 * this interface. Following the Map and Shuffle phases, the Worker node
 * will group all intermediate values associated with a specific key and invoke
 * the {@link #reduce(String, Iterator)} method to aggregate the results.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.1
 * @since 2026-03-30
 */
public interface Reducer {

    /**
     * Aggregates a stream of intermediate values that share the same key.
     * <p>
     * This method is called once for each distinct key present in the specific
     * partition assigned to the executing Worker. The implementation should process
     * the stream of values (e.g., summing numbers, concatenating strings) and emit
     * a final consolidated result.
     * </p>
     * <p>
     * <b>Note:</b> An {@link Iterator} is utilized rather than a standard List
     * to prevent {@link OutOfMemoryError} exceptions when dealing with highly
     * skewed data distributions (e.g., millions of values for a single key).
     * </p>
     *
     * @param key    The intermediate key shared by all values in the provided stream.
     * @param values An {@link Iterator} of {@link String} values aggregated from the Map phase
     * that correspond to the provided key. Data is streamed directly from disk.
     * @return A single {@link KeyValuePair} representing the final aggregated output
     * for the given key, or {@code null} if no output should be emitted.
     */
    KeyValuePair reduce(String key, Iterator<String> values);
}