package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enterprise-grade external merge sort utility for distributed data processing.
 * <p>
 * To prevent OutOfMemoryError (OOM) exceptions on highly skewed or massive
 * data partitions, this engine processes data using disk-backed buffers.
 * The process is strictly divided into two phases:
 * <ol>
 * <li><b>Chunking & Sorting:</b> Raw streams are read into constrained memory buffers.
 * Once full, the buffer is sorted and written back to disk as a "Sorted Run".</li>
 * <li><b>K-Way Merge & Reduce:</b> All sorted runs are opened simultaneously. A Priority
 * Queue extracts the absolute minimum key across all files, groups identical keys,
 * and streams them directly into the user's {@link Reducer}.</li>
 * </ol>
 * </p>
 * <p>
 * <b>Encoding Note:</b> All I/O operations strictly enforce UTF-8 encoding to
 * maintain byte-boundary safety and optimize disk footprint.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @since 2026-04-24
 */
public class ExternalMergeSorter {

    private static final Logger logger = LoggerFactory.getLogger(ExternalMergeSorter.class);

    // Limit in-memory buffer to ~500,000 records per chunk to ensure safety within container bounds
    private static final int CHUNK_RECORD_LIMIT = 500_000;

    /**
     * Orchestrates the entire Spill-to-Disk sort and reduce pipeline.
     *
     * """
     * Executes an external merge sort on raw partition data and applies the reduce logic.
     * * Args:
     * rawDataDir (Path): The local directory containing the raw gRPC stream files.
     * reducer (Reducer): The user-defined Reducer instance dynamically loaded into the JVM.
     * pool (ForkJoinPool): The shared thread pool, utilized here for parallel chunk sorting.
     * * Returns:
     * Path: The absolute path to the final serialized file containing the reduced output.
     * * Raises:
     * IOException: If disk I/O fails during chunking, merging, or cleanup.
     * """
     */
    public static Path sortReduceAndSpill(Path rawDataDir, Reducer reducer, ForkJoinPool pool) throws IOException {
        logger.info("Initializing ExternalMergeSorter on directory: {}", rawDataDir);

        Path runsDir = rawDataDir.resolve("sorted_runs");
        Files.createDirectories(runsDir);

        // Phase 1: Create Sorted Runs
        List<Path> sortedRuns = createSortedRuns(rawDataDir, runsDir);
        logger.info("Generated {} sorted runs on disk. Commencing K-Way Merge...", sortedRuns.size());

        // Phase 2: K-Way Merge and Execute Reduce
        Path finalOutputFile = rawDataDir.resolve("final_reduced_output.txt");
        performNWayMergeAndReduce(sortedRuns, finalOutputFile, reducer);

        return finalOutputFile;
    }

    /**
     * Reads raw input files, sorts them in memory up to a safe threshold, and spills to disk.
     *
     * """
     * Scans all raw stream files in the target directory, grouping data into memory-safe chunks.
     * * Each chunk is sorted alphabetically by Key and written to an isolated 'run' file.
     * * Args:
     * rawDataDir (Path): The directory containing raw gRPC `.txt` streams.
     * runsDir (Path): The directory where sorted chunks will be stored.
     * * Returns:
     * List[Path]: A list of file paths pointing to the successfully sorted runs.
     * """
     */
    private static List<Path> createSortedRuns(Path rawDataDir, Path runsDir) throws IOException {
        List<Path> sortedRunFiles = new ArrayList<>();
        List<KeyValuePair> currentChunk = new ArrayList<>(CHUNK_RECORD_LIMIT);
        int runCounter = 0;

        try (Stream<Path> rawFiles = Files.list(rawDataDir)) {
            List<Path> filesToProcess = rawFiles
                    .filter(p -> p.getFileName().toString().startsWith("grpc_stream_"))
                    .collect(Collectors.toList());

            for (Path rawFile : filesToProcess) {
                try (BufferedReader reader = Files.newBufferedReader(rawFile, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;

                        // Fast extraction based on the \t delimiter
                        int tabIndex = line.indexOf('\t');
                        if (tabIndex > 0) {
                            String key = line.substring(0, tabIndex);
                            String value = line.substring(tabIndex + 1);
                            currentChunk.add(new KeyValuePair(key, value));
                        }

                        // Flush to disk when memory threshold is met
                        if (currentChunk.size() >= CHUNK_RECORD_LIMIT) {
                            sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter++));
                            currentChunk.clear();
                        }
                    }
                }
            }

            // Flush any remaining data in the buffer
            if (!currentChunk.isEmpty()) {
                sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter));
            }
        }
        return sortedRunFiles;
    }

    /**
     * Sorts an in-memory chunk and writes it securely to disk using UTF-8.
     *
     * """
     * In-memory sorting utility for individual data chunks.
     * * Args:
     * chunk (List[KeyValuePair]): The loaded data records.
     * runsDir (Path): Destination directory for the sorted file.
     * runId (int): Identifier for naming the output run file.
     * * Returns:
     * Path: The path to the successfully written run file.
     * """
     */
    private static Path sortAndSpillChunk(List<KeyValuePair> chunk, Path runsDir, int runId) throws IOException {
        // Sort alphabetically by Key to guarantee Total Order partitioning
        chunk.sort(Comparator.comparing(KeyValuePair::key));

        Path runFile = runsDir.resolve("run_" + runId + ".txt");
        try (BufferedWriter writer = Files.newBufferedWriter(runFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (KeyValuePair pair : chunk) {
                writer.write(pair.key() + "\t" + pair.value() + "\n");
            }
        }
        logger.debug("Spilled sorted run to disk: {} ({} records)", runFile.getFileName(), chunk.size());
        return runFile;
    }

    /**
     * Merges multiple sorted files and streams grouped keys to the user's Reducer.
     *
     * """
     * Implements a K-Way Merge using a PriorityQueue across open file streams.
     * * Identical keys extracted from across different runs are grouped into a single list
     * and streamed directly into the Reducer to minimize memory usage.
     * * Args:
     * sortedRuns (List[Path]): The collection of sorted run files to merge.
     * outputFile (Path): The target destination for the final reduced output.
     * reducer (Reducer): The user's aggregation logic.
     * """
     */
    private static void performNWayMergeAndReduce(List<Path> sortedRuns, Path outputFile, Reducer reducer) throws IOException {
        PriorityQueue<StreamNode> pq = new PriorityQueue<>(Comparator.comparing(n -> n.currentPair.key()));
        List<BufferedReader> activeReaders = new ArrayList<>();

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // 1. Initialize streams: Read the first line of every sorted run into the Priority Queue
            for (Path runFile : sortedRuns) {
                BufferedReader reader = Files.newBufferedReader(runFile, StandardCharsets.UTF_8);
                activeReaders.add(reader);
                StreamNode initialNode = StreamNode.fromReader(reader);
                if (initialNode != null) {
                    pq.add(initialNode);
                }
            }

            String currentGroupingKey = null;
            List<String> currentValuesGroup = new ArrayList<>();

            // 2. Continually pop the globally smallest key from the Priority Queue
            while (!pq.isEmpty()) {
                StreamNode minNode = pq.poll();
                KeyValuePair pair = minNode.currentPair;

                if (currentGroupingKey == null) {
                    currentGroupingKey = pair.key();
                }

                // If the key changes, it's time to reduce the previously grouped block
                if (!currentGroupingKey.equals(pair.key())) {
                    executeReduceAndWrite(currentGroupingKey, currentValuesGroup, reducer, writer);

                    // Reset group for the new key
                    currentGroupingKey = pair.key();
                    currentValuesGroup.clear();
                }

                currentValuesGroup.add(pair.value());

                // Read the next line from the file we just extracted from
                StreamNode nextNode = StreamNode.fromReader(minNode.reader);
                if (nextNode != null) {
                    pq.add(nextNode);
                }
            }

            // Flush the final grouped key
            if (currentGroupingKey != null && !currentValuesGroup.isEmpty()) {
                executeReduceAndWrite(currentGroupingKey, currentValuesGroup, reducer, writer);
            }

        } finally {
            // Guarantee closure of all file descriptors to prevent IO leaks
            for (BufferedReader reader : activeReaders) {
                try { reader.close(); } catch (IOException ignored) {}
            }
        }
    }

    /**
     * Helper to isolate the execution of the user's reducer and persistence layer.
     */
    private static void executeReduceAndWrite(String key, List<String> values, Reducer reducer, BufferedWriter writer) throws IOException {
        KeyValuePair reducedResult = reducer.reduce(key, values);
        if (reducedResult != null) {
            writer.write(reducedResult.key() + "\t" + reducedResult.value() + "\n");
        }
    }

    /**
     * Recursively deletes a local directory.
     *
     * """
     * Cleans up all temporary chunk files and the root directory after successful S3 upload.
     * * Args:
     * directoryToBeDeleted (Path): Target directory to purge.
     * """
     */
    public static void cleanupDirectory(Path directoryToBeDeleted) {
        try (Stream<Path> walk = Files.walk(directoryToBeDeleted)) {
            walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
            logger.info("Successfully cleaned up temporary directory: {}", directoryToBeDeleted);
        } catch (IOException e) {
            logger.warn("Failed to clean up temporary directory {}. Space may be leaked.", directoryToBeDeleted, e);
        }
    }

    /**
     * Internal container tracking the state of an open file stream during the K-Way merge.
     */
    private static class StreamNode {
        KeyValuePair currentPair;
        BufferedReader reader;

        StreamNode(KeyValuePair currentPair, BufferedReader reader) {
            this.currentPair = currentPair;
            this.reader = reader;
        }

        static StreamNode fromReader(BufferedReader reader) throws IOException {
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return null; // EOF reached for this specific run
            }
            int tabIndex = line.indexOf('\t');
            if (tabIndex > 0) {
                return new StreamNode(new KeyValuePair(line.substring(0, tabIndex), line.substring(tabIndex + 1)), reader);
            }
            return null; // Malformed line failsafe
        }
    }
}