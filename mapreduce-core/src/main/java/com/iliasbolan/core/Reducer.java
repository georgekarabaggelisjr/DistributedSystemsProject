package com.iliasbolan.core;

import java.util.List;

/**
 * The core contract for the Reduce phase of a Map-Reduce job.
 * <p>
 * Users submitting a compute job must provide a compiled Java class that implements
 * this interface. Following the Map and Shuffle phases, the Worker node
 * will group all intermediate values associated with a specific key and invoke
 * the {@link #reduce(String, List)} method to aggregate the results.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-03-30
 */
public interface Reducer {

    /**
     * Aggregates a list of intermediate values that share the same key.
     * <p>
     * This method is called once for each distinct key present in the specific
     * partition assigned to the executing Worker. The implementation should process
     * the list of values (e.g., summing numbers, concatenating strings) and emit
     * a final consolidated result.
     * </p>
     *
     * @param key    The intermediate key shared by all values in the provided list.
     * @param values A {@link List} of {@link String} values aggregated from the Map phase
     * that correspond to the provided key.
     * @return A single {@link KeyValuePair} representing the final aggregated output
     * for the given key.
     */
    KeyValuePair reduce(String key, List<String> values);
}