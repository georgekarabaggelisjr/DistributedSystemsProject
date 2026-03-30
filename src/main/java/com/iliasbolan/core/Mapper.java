package com.iliasbolan.core;

import java.util.List;

/**
 * The core contract for the Map phase of a Map-Reduce job.
 * <p>
 * Users submitting a compute job must provide a compiled Java class that implements
 * this interface. The Worker node's execution engine will dynamically load
 * the user's implementation at runtime using Java Reflection and invoke the {@link #map(String, String)}
 * method concurrently across multiple threads via the Fork/Join Framework.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @since 2026-03-30
 */
public interface Mapper {

    /**
     * Processes a single input key-value pair and produces a list of intermediate key-value pairs.
     * <p>
     * In a standard text-processing job, the key might be the line number or file offset,
     * and the value would be the contents of the line. The implementation is responsible
     * for parsing the input and emitting the appropriate intermediate mapping.
     * </p>
     *
     * @param key   The input key (e.g., byte offset or document identifier).
     * @param value The input value (e.g., a line of text or a JSON object).
     * @return A {@link List} of {@link KeyValuePair} objects representing the intermediate
     * data to be shuffled and routed to the Reducers.
     */
    List<KeyValuePair> map(String key, String value);
}