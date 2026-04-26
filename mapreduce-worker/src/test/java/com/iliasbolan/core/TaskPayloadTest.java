package com.iliasbolan.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for the {@link TaskPayload} record.
 * <p>
 * Validates that the Jackson JSON parser correctly maps incoming RabbitMQ
 * messages to our strict Java 17 record structure.
 * </p>
 * <p>
 * <b>Security & Locality Update:</b><br>
 * Now verifies the deserialization of the <code>jobToken</code> for ESS
 * authorization and ensuring the record supports the updated 3.0 schema.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-04-25
 * @see com.iliasbolan.core.TaskPayload
 */
class TaskPayloadTest {

    /**
     * Verifies that all fields, including security tokens and collection-based
     * routing tables, are mapped correctly from a JSON payload.
     *
     * @throws Exception If the JSON string is malformed or types are incompatible.
     */
    @Test
    void testJsonDeserialization_MapsAllFieldsCorrectly() throws Exception {
        // Arrange: Updated JSON reflecting the 3.0 Security Schema
        String jsonPayload = """
                {
                    "jobId": "job-2026",
                    "taskId": "reduce-partition-0",
                    "jobToken": "secure-hmac-token-xyz",
                    "taskType": "REDUCE",
                    "bucketName": "system-input",
                    "objectName": "dataset.csv",
                    "byteOffset": 0,
                    "byteLength": 0,
                    "numReducers": 4,
                    "userCodeBucket": "user-code",
                    "userCodeObject": "WordCountReducer.class",
                    "className": "com.example.WordCountReducer",
                    "workerEndpoints": ["10.0.1.45:7337", "10.0.1.46:7337"]
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();

        // Act
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);

        // Assert
        assertNotNull(payload, "Payload should not be null after deserialization");
        assertEquals("job-2026", payload.jobId());
        assertEquals("reduce-partition-0", payload.taskId());
        assertEquals("secure-hmac-token-xyz", payload.jobToken()); // NEW: Token validation
        assertEquals("REDUCE", payload.taskType());
        assertEquals(4, payload.numReducers());

        // Verify Peer-to-Peer Shuffle routing table mapping
        assertNotNull(payload.workerEndpoints(), "workerEndpoints list should be initialized");
        assertEquals(2, payload.workerEndpoints().size());
        assertEquals(List.of("10.0.1.45:7337", "10.0.1.46:7337"), payload.workerEndpoints());
    }
}