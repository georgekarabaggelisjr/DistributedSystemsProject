package com.iliasbolan.engine;

import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link DynamicClassLoader}.
 * This verifies that the reflection-based loading logic correctly validates
 * class types and handles missing resources gracefully.
 * NOTE: To test a "Success" case for loading an actual Mapper/Reducer,
 * We would need to have a compiled .class file at a specific path
 * that is NOT already in the system classpath.
 * In a production CI pipeline, we would often compile a "DummyMapper.java"
 * to a temp folder during the test setup to verify the success path.
 */
class DynamicClassLoaderTest {

    // Use the project's own build directory to find classes for testing
    private final String currentClasspath = new File("target/classes").getAbsolutePath();

    @Test
    void testLoadMapper_Failure_ClassDoesNotImplementInterface() {
        // Arrange: Use a class that exists (TaskExecutor) but is NOT a Mapper
        String className = "com.iliasbolan.engine.TaskExecutor";

        // Act & Assert: It should throw IllegalArgumentException during validation
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                DynamicClassLoader.loadMapper(currentClasspath, className)
        );

        assertTrue(exception.getMessage().contains("does not implement the Mapper interface"));
    }

    @Test
    void testLoadReducer_Failure_ClassDoesNotImplementInterface() {
        // Arrange: Use a class that exists but is NOT a Reducer
        String className = "com.iliasbolan.engine.TaskExecutor";

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                DynamicClassLoader.loadReducer(currentClasspath, className)
        );

        assertTrue(exception.getMessage().contains("does not implement the Reducer interface"));
    }

    @Test
    void testLoadClass_Failure_ClassNotFound() {
        // Arrange: A class name that definitely doesn't exist
        String className = "com.iliasbolan.NonExistentClass";

        // Act & Assert: Should throw the native ClassNotFoundException
        assertThrows(ClassNotFoundException.class, () ->
                DynamicClassLoader.loadMapper(currentClasspath, className)
        );
    }

    @Test
    void testLoadClass_Failure_InvalidDirectory() {
        // Arrange: A directory path that doesn't exist
        String invalidPath = "/tmp/folder/that/is/not/here";
        String className = "SomeClass";

        // Act & Assert: ClassNotFoundException is expected because the loader
        // won't find the class in the non-existent directory.
        assertThrows(ClassNotFoundException.class, () ->
                DynamicClassLoader.loadMapper(invalidPath, className)
        );
    }
}