package com.iliasbolan.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for the {@link TaskPayload} record.
 * This ensures that the Jackson JSON parser correctly maps incoming RabbitMQ
 * messages to our strict Java 17 record structure.
 */
class TaskPayloadTest {

    @Test
    void testJsonDeserialization_MapsAllFieldsCorrectly() throws Exception {
        // Arrange: A simulated JSON string exactly like the Manager Service would send
        String jsonPayload = """
                {
                    "jobId": "job-2026",
                    "taskId": "map-task-001",
                    "taskType": "MAP",
                    "bucketName": "system-input",
                    "objectName": "dataset.csv",
                    "byteOffset": 1024,
                    "byteLength": 67108864,
                    "numReducers": 4,
                    "userCodeBucket": "user-code",
                    "userCodeObject": "WordCountMapper.class",
                    "className": "com.example.WordCountMapper"
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();

        // Act: Attempt to convert the raw JSON string into the Java record
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);

        // Assert: Verify every single field mapped correctly and data types align
        assertNotNull(payload, "Payload should not be null after deserialization");
        assertEquals("job-2026", payload.jobId());
        assertEquals("map-task-001", payload.taskId());
        assertEquals("MAP", payload.taskType());
        assertEquals("system-input", payload.bucketName());
        assertEquals("dataset.csv", payload.objectName());
        assertEquals(1024L, payload.byteOffset());
        assertEquals(67108864L, payload.byteLength()); // Proves 'long' parsing works
        assertEquals(4, payload.numReducers());        // Proves 'int' parsing works
        assertEquals("user-code", payload.userCodeBucket());
        assertEquals("WordCountMapper.class", payload.userCodeObject());
        assertEquals("com.example.WordCountMapper", payload.className());
    }
}