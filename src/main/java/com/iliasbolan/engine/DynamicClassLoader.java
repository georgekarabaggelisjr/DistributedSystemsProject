package com.iliasbolan.engine;

import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * A utility class responsible for dynamically loading user-provided Map and Reduce classes at runtime.
 * <p>
 * In a distributed Map-Reduce architecture, Worker nodes are generic compute engines.
 * They receive compiled user code (typically a {@code .class} or {@code .jar} file) from a
 * Shared File System and must load this code into the JVM dynamically. This class utilizes
 * {@link URLClassLoader} and Java Reflection to instantiate the user's logic and cast it
 * to the system's strict {@link Mapper} or {@link Reducer} interfaces.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.1
 * @since 2026-03-30
 */
public class DynamicClassLoader {

    // Instantiate the SLF4J Logger specific to this class
    private static final Logger logger = LoggerFactory.getLogger(DynamicClassLoader.class);

    /**
     * Dynamically loads a Java class from a specified file path and attempts to instantiate it
     * as a {@link Mapper}.
     *
     * @param directoryPath The absolute or relative path to the directory containing the compiled class.
     * @param className     The fully qualified name of the class (e.g., "com.user.MyMapper").
     * @return An instantiated object that implements the {@link Mapper} interface.
     * @throws Exception If the file cannot be found, the class cannot be loaded, or it does not implement {@link Mapper}.
     */
    public static Mapper loadMapper(String directoryPath, String className) throws Exception {
        logger.info("Attempting to load Mapper class '{}' from directory: {}", className, directoryPath);

        Class<?> loadedClass = loadClassFromFile(directoryPath, className);

        // Ensure the user's class actually implements our Mapper interface
        if (!Mapper.class.isAssignableFrom(loadedClass)) {
            logger.error("Validation failed: Class '{}' does not implement the core Mapper interface.", className);
            throw new IllegalArgumentException("The provided class does not implement the Mapper interface.");
        }

        // Instantiate and cast
        Mapper mapperInstance = (Mapper) loadedClass.getDeclaredConstructor().newInstance();
        logger.info("Successfully instantiated Mapper: {}", className);

        return mapperInstance;
    }

    /**
     * Dynamically loads a Java class from a specified file path and attempts to instantiate it
     * as a {@link Reducer}.
     *
     * @param directoryPath The absolute or relative path to the directory containing the compiled class.
     * @param className     The fully qualified name of the class (e.g., "com.user.MyReducer").
     * @return An instantiated object that implements the {@link Reducer} interface.
     * @throws Exception If the file cannot be found, the class cannot be loaded, or it does not implement {@link Reducer}.
     */
    public static Reducer loadReducer(String directoryPath, String className) throws Exception {
        logger.info("Attempting to load Reducer class '{}' from directory: {}", className, directoryPath);

        Class<?> loadedClass = loadClassFromFile(directoryPath, className);

        // Ensure the user's class actually implements our Reducer interface
        if (!Reducer.class.isAssignableFrom(loadedClass)) {
            logger.error("Validation failed: Class '{}' does not implement the core Reducer interface.", className);
            throw new IllegalArgumentException("The provided class does not implement the Reducer interface.");
        }

        // Instantiate and cast
        Reducer reducerInstance = (Reducer) loadedClass.getDeclaredConstructor().newInstance();
        logger.info("Successfully instantiated Reducer: {}", className);

        return reducerInstance;
    }

    /**
     * Core reflection logic to load a class into the JVM from a file directory.
     *
     * @param directoryPath The path to the directory containing the {@code .class} files.
     * @param className     The fully qualified name of the class to load.
     * @return The loaded {@link Class} object.
     * @throws MalformedURLException If the directory path cannot be converted to a valid URL.
     * @throws ClassNotFoundException If the specific class name cannot be found in the directory.
     * @throws IOException If an I/O error occurs while closing the URLClassLoader.
     */
    private static Class<?> loadClassFromFile(String directoryPath, String className)
            throws MalformedURLException, ClassNotFoundException, IOException {

        File file = new File(directoryPath);

        if (!file.exists() || !file.isDirectory()) {
            logger.warn("The directory '{}' does not exist or is not a valid directory. Class loading may fail.", directoryPath);
        }

        // Convert the file path to a URL format required by URLClassLoader
        URL url = file.toURI().toURL();
        URL[] urls = new URL[]{url};

        // Create a new ClassLoader pointed at the specific directory
        try (URLClassLoader classLoader = new URLClassLoader(urls, DynamicClassLoader.class.getClassLoader())) {
            // Load the class into the JVM
            return classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            logger.error("Failed to find class '{}' inside directory '{}'", className, directoryPath, e);
            throw e;
        }
    }
}