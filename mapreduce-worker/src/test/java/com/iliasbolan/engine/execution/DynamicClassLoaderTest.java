package com.iliasbolan.engine.execution;

import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.engine.TaskExecutor;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link DynamicClassLoader}.
 * <p>
 * This suite verifies that the reflection-based loading logic correctly validates
 * class types and handles missing resources gracefully within the isolated
 * sandbox environment.
 * </p>
 * <p>
 * <b>Sandbox Integration Note:</b><br>
 * These tests simulate the staging directory structure used by the {@link SandboxRunner}.
 * By utilizing the project's own build directory, we can verify the validation logic
 * against known types while ensuring that the <code>URLClassLoader</code> correctly
 * isolates the loading context.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @see DynamicClassLoader
 */
class DynamicClassLoaderTest {

    /** * Use the project's own build directory to find classes for testing.
     * In a sandbox environment, this corresponds to the staged bytecode directory.
     */
    private final String currentClasspath = new File("target/classes").getAbsolutePath();

    /**
     * Verifies that the loader correctly rejects classes that do not implement
     * the {@link Mapper} interface.
     * <p>
     * This test uses the {@link TaskExecutor} class as a known existing class
     * that fails the interface validation.
     * </p>
     */
    @Test
    void testLoadMapper_Failure_ClassDoesNotImplementInterface() {
        // Arrange: Use a class that exists but is NOT a Mapper
        String className = "com.iliasbolan.engine.TaskExecutor";

        // Act & Assert: It should throw IllegalArgumentException during validation
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                DynamicClassLoader.loadMapper(currentClasspath, className)
        );

        assertTrue(exception.getMessage().contains("does not implement the Mapper interface"),
                "Exception message should indicate interface validation failure.");
    }

    /**
     * Verifies that the loader correctly rejects classes that do not implement
     * the {@link Reducer} interface.
     */
    @Test
    void testLoadReducer_Failure_ClassDoesNotImplementInterface() {
        // Arrange: Use a class that exists but is NOT a Reducer
        String className = "com.iliasbolan.engine.TaskExecutor";

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                DynamicClassLoader.loadReducer(currentClasspath, className)
        );

        assertTrue(exception.getMessage().contains("does not implement the Reducer interface"),
                "Exception message should indicate interface validation failure.");
    }

    /**
     * Verifies that the loader correctly propagates a {@link ClassNotFoundException}
     * when an invalid class name is provided.
     */
    @Test
    void testLoadClass_Failure_ClassNotFound() {
        // Arrange: A class name that definitely doesn't exist
        String className = "com.iliasbolan.NonExistentClass";

        // Act & Assert: Should throw the native ClassNotFoundException
        assertThrows(ClassNotFoundException.class, () ->
                DynamicClassLoader.loadMapper(currentClasspath, className)
        );
    }

    /**
     * Verifies that the loader handles non-existent directories gracefully.
     * <p>
     * While the loader logs a warning for missing directories, the actual
     * loading attempt should still result in a {@link ClassNotFoundException}.
     * </p>
     */
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