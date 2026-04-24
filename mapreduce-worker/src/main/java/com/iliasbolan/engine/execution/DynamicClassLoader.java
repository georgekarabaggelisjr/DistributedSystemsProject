package com.iliasbolan.engine.execution;

import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * A utility class responsible for dynamically loading user-provided Map and Reduce classes at runtime.
 * <p>
 * This class is a core component of the worker's security boundary. It is designed to run
 * exclusively within the isolated {@link SandboxRunner} JVM. By loading untrusted third-party
 * bytecode into an ephemeral process, the system prevents Metaspace memory leaks and
 * protects the primary worker daemon from malicious code execution.
 * </p>
 * <p>
 * The loader utilizes the Java Reflection API to instantiate the user's logic and cast it
 * to the system's strict {@link Mapper} or {@link Reducer} interfaces.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-03-30
 */
public class DynamicClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(DynamicClassLoader.class);

    /**
     * Dynamically loads and instantiates a {@link Mapper} implementation from the filesystem.
     *
     * @param directoryPath The local path where the bytecode was staged by the Orchestrator.
     * @param className     The fully qualified name of the user's Mapper class.
     * @return An instantiated and validated {@link Mapper} object.
     * @throws IllegalArgumentException If the loaded class does not implement the Mapper interface.
     * @throws Exception                If the class cannot be found or instantiation fails.
     */
    public static Mapper loadMapper(String directoryPath, String className) throws Exception {
        logger.info("Sandbox: Attempting to load Mapper class '{}' from {}", className, directoryPath);

        Class<?> loadedClass = loadClassFromFile(directoryPath, className);

        // Ensure the user's class actually implements our Mapper interface
        if (!Mapper.class.isAssignableFrom(loadedClass)) {
            logger.error("Sandbox Validation Error: Class '{}' does not implement Mapper.", className);
            throw new IllegalArgumentException("The provided class does not implement the Mapper interface.");
        }

        // Instantiate using the default no-args constructor via reflection
        return (Mapper) loadedClass.getDeclaredConstructor().newInstance();
    }

    /**
     * Dynamically loads and instantiates a {@link Reducer} implementation from the filesystem.
     *
     * @param directoryPath The local path where the bytecode was staged by the Orchestrator.
     * @param className     The fully qualified name of the user's Reducer class.
     * @return An instantiated and validated {@link Reducer} object.
     * @throws IllegalArgumentException If the loaded class does not implement the Reducer interface.
     * @throws Exception                If the class cannot be found or instantiation fails.
     */
    public static Reducer loadReducer(String directoryPath, String className) throws Exception {
        logger.info("Sandbox: Attempting to load Reducer class '{}' from {}", className, directoryPath);

        Class<?> loadedClass = loadClassFromFile(directoryPath, className);

        // Ensure the user's class actually implements our Reducer interface
        if (!Reducer.class.isAssignableFrom(loadedClass)) {
            logger.error("Sandbox Validation Error: Class '{}' does not implement Reducer.", className);
            throw new IllegalArgumentException("The provided class does not implement the Reducer interface.");
        }

        // Instantiate using the default no-args constructor
        return (Reducer) loadedClass.getDeclaredConstructor().newInstance();
    }

    /**
     * Core reflection logic to load a class into the JVM from a file directory.
     * <p>
     * This method initializes a {@link URLClassLoader} scoped to the task's staged bytecode
     * directory. It uses a try-with-resources block to ensure the ClassLoader is marked
     * for closure immediately after the Class object is successfully loaded.
     * </p>
     *
     * @param directoryPath The path to the directory containing the {@code .class} files.
     * @param className     The fully qualified name of the class to load.
     * @return The loaded {@link Class} object.
     * @throws Exception If the path is invalid or the class is not found.
     */
    private static Class<?> loadClassFromFile(String directoryPath, String className) throws Exception {
        File file = new File(directoryPath);

        if (!file.exists() || !file.isDirectory()) {
            logger.warn("Sandbox I/O Warning: Staging directory '{}' missing or invalid.", directoryPath);
        }

        // Convert the local file path to a URL format required by URLClassLoader
        URL url = file.toURI().toURL();
        URL[] urls = new URL[]{url};

        // Standard parent-last loading to prefer staged bytecode over worker internals
        try (URLClassLoader classLoader = new URLClassLoader(urls, DynamicClassLoader.class.getClassLoader())) {
            return classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            logger.error("Sandbox Runtime Error: Class '{}' not found in path {}", className, directoryPath);
            throw e;
        }
    }
}