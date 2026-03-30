package com.iliasbolan.engine;

import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;

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
 * @version 1.0
 * @since 2026-03-30
 */
public class DynamicClassLoader {

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
        Class<?> loadedClass = loadClassFromFile(directoryPath, className);

        // Ensure the user's class actually implements our Mapper interface
        if (!Mapper.class.isAssignableFrom(loadedClass)) {
            throw new IllegalArgumentException("The provided class does not implement the Mapper interface.");
        }

        // Instantiate and cast
        return (Mapper) loadedClass.getDeclaredConstructor().newInstance();
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
        Class<?> loadedClass = loadClassFromFile(directoryPath, className);

        // Ensure the user's class actually implements our Reducer interface
        if (!Reducer.class.isAssignableFrom(loadedClass)) {
            throw new IllegalArgumentException("The provided class does not implement the Reducer interface.");
        }

        // Instantiate and cast
        return (Reducer) loadedClass.getDeclaredConstructor().newInstance();
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

        // Convert the file path to a URL format required by URLClassLoader
        URL url = file.toURI().toURL();
        URL[] urls = new URL[]{url};

        // Create a new ClassLoader pointed at the specific directory
        try (URLClassLoader classLoader = new URLClassLoader(urls, DynamicClassLoader.class.getClassLoader())) {
            // Load the class into the JVM
            return classLoader.loadClass(className);
        }
    }
}