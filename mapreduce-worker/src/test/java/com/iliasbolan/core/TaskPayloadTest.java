package com.iliasbolan.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for the {@link TaskPayload} record.
 * <p>
 * This ensures that the Jackson JSON parser correctly maps incoming RabbitMQ
 * messages to our strict Java 17 record structure. These tests specifically
 * validate the integrity of the data contract between the Python Manager
 * and the Java Worker nodes.
 * </p>
 * <p>
 * <b>P2P Shuffle Update:</b><br>
 * Validates the deserialization of the <code>workerEndpoints</code> routing table,
 * ensuring the list of sibling node addresses is correctly mapped for the
 * Peer-to-Peer data fetch logic introduced in the 2.0 architecture.
 * </p>
 *
 * @author Ilias Bolanakis
 * @see com.iliasbolan.core.TaskPayload
 */
class TaskPayloadTest {

    /**
     * Verifies that all fields, including primitive types and collection-based
     * routing tables, are mapped correctly from a JSON payload.
     * <p>
     * This test ensures that Jackson correctly handles the transition from
     * snake_case or camelCase JSON keys to the immutable fields of the Java record.
     * </p>
     *
     * @throws Exception If the JSON string is malformed or types are incompatible.
     */
    @Test
    void testJsonDeserialization_MapsAllFieldsCorrectly() throws Exception {
        // Arrange: A simulated JSON string reflecting the P2P Shuffle schema
        String jsonPayload = """
                {
                    "jobId": "job-2026",
                    "taskId": "reduce-partition-0",
                    "taskType": "REDUCE",
                    "bucketName": "system-input",
                    "objectName": "dataset.csv",
                    "byteOffset": 0,
                    "byteLength": 0,
                    "numReducers": 4,
                    "userCodeBucket": "user-code",
                    "userCodeObject": "WordCountReducer.class",
                    "className": "com.example.WordCountReducer",
                    "workerEndpoints": ["10.0.1.45:8080", "10.0.1.46:8080"]
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();

        // Act: Attempt to convert the raw JSON string into the Java record
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);

        // Assert: Verify every single field mapped correctly and data types align
        assertNotNull(payload, "Payload should not be null after deserialization");
        assertEquals("job-2026", payload.jobId());
        assertEquals("reduce-partition-0", payload.taskId());
        assertEquals("REDUCE", payload.taskType());
        assertEquals("system-input", payload.bucketName());
        assertEquals("dataset.csv", payload.objectName());
        assertEquals(0L, payload.byteOffset());
        assertEquals(0L, payload.byteLength()); // Proves 'long' parsing works
        assertEquals(4, payload.numReducers());        // Proves 'int' parsing works
        assertEquals("user-code", payload.userCodeBucket());
        assertEquals("WordCountReducer.class", payload.userCodeObject());
        assertEquals("com.example.WordCountReducer", payload.className());

        // Verify Peer-to-Peer Shuffle routing table mapping
        assertNotNull(payload.workerEndpoints(), "workerEndpoints list should be initialized");
        assertEquals(2, payload.workerEndpoints().size(), "Should contain exactly 2 endpoints");
        assertEquals(List.of("10.0.1.45:8080", "10.0.1.46:8080"), payload.workerEndpoints());
    }
}