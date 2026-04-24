package com.iliasbolan.engine.shuffle;

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
 * External merge sort utility for distributed data processing.
 *
 * <p>To prevent {@link OutOfMemoryError} (OOM) exceptions on highly skewed or massive
 * data partitions, this engine processes data using disk-backed buffers.
 * The process is strictly divided into two distinct phases:
 * <ol>
 * <li><b>Chunking &amp; Sorting:</b> Raw streams are read into constrained memory buffers.
 * Once the buffer reaches capacity, it is sorted and persisted to disk as a "Sorted Run".</li>
 * <li><b>K-Way Merge &amp; Reduce:</b> All sorted runs are opened concurrently. A {@link PriorityQueue}
 * is utilized to extract the absolute minimum key across all active runs, grouping identical keys
 * into collections that are streamed directly into the user-defined {@link Reducer}.</li>
 * </ol>
 * </p>
 *
 * <p><b>Character Encoding:</b> All I/O operations strictly enforce UTF-8 encoding to
 * guarantee byte-boundary safety and optimize the storage footprint on the local container disk.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 */
public class ExternalMergeSorter {

    private static final Logger logger = LoggerFactory.getLogger(ExternalMergeSorter.class);

    /** * Limits the in-memory buffer to 500,000 records per chunk.
     * This threshold ensures that the JVM remains within the memory limits
     * typically assigned to Kubernetes worker containers.
     */
    private static final int CHUNK_RECORD_LIMIT = 500_000;

    /**
     * Orchestrates the complete Spill-to-Disk sort and reduce pipeline.
     *
     * <p>This method initializes the external merge sort on raw partition data
     * and subsequently applies the user's reduction logic to the sorted streams.</p>
     *
     * @param rawDataDir The local directory containing the raw gRPC stream files.
     * @param reducer    The user-defined Reducer implementation dynamically loaded into the JVM.
     * @param pool       The shared thread pool (Note: Currently reserved for future parallel expansion).
     * @return The {@link Path} to the final serialized file containing the reduced output.
     * @throws IOException If a disk I/O failure occurs during chunking, merging, or resource cleanup.
     */
    @SuppressWarnings("unused")
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
     * Reads raw input files and spills sorted chunks to the local file system.
     *
     * <p>Scans all gRPC stream files in the target directory and groups data into
     * memory-safe segments. Each segment is sorted alphabetically by Key and
     * written to an isolated 'run' file to ensure total order partitioning.</p>
     *
     * @param rawDataDir The directory containing raw gRPC stream files.
     * @param runsDir    The directory where sorted chunk files will be persisted.
     * @return A {@link List} of {@link Path} objects identifying the generated sorted runs.
     * @throws IOException If disk I/O failures occur while reading streams or spilling runs.
     */
    @SuppressWarnings("SimplifyStreamApiCallChains")
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

                        int tabIndex = line.indexOf('\t');
                        if (tabIndex > 0) {
                            String key = line.substring(0, tabIndex);
                            String value = line.substring(tabIndex + 1);
                            currentChunk.add(new KeyValuePair(key, value));
                        }

                        if (currentChunk.size() >= CHUNK_RECORD_LIMIT) {
                            sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter++));
                            currentChunk.clear();
                        }
                    }
                }
            }

            if (!currentChunk.isEmpty()) {
                sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter));
            }
        }
        return sortedRunFiles;
    }

    /**
     * Sorts an in-memory chunk and persists it to disk using UTF-8 encoding.
     *
     * @param chunk   The collection of records currently loaded in memory.
     * @param runsDir The destination directory for the sorted file.
     * @param runId   A unique identifier for naming the resulting run file.
     * @return The {@link Path} to the persisted sorted run.
     * @throws IOException If a disk I/O failure occurs during the write operation.
     */
    private static Path sortAndSpillChunk(List<KeyValuePair> chunk, Path runsDir, int runId) throws IOException {
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
     * Merges multiple sorted runs and streams grouped keys to the user-defined Reducer.
     *
     * <p>Utilizes a K-Way Merge algorithm powered by a {@link PriorityQueue} across
     * multiple open file streams. Identical keys from different runs are grouped
     * into a single collection and streamed directly to the Reducer to maintain
     * a minimal memory footprint.</p>
     *
     * @param sortedRuns The collection of sorted run files to be merged.
     * @param outputFile The destination file for the final reduced results.
     * @param reducer    The user-defined aggregation logic.
     * @throws IOException If disk I/O failures occur during merging or reducing.
     */
    private static void performNWayMergeAndReduce(List<Path> sortedRuns, Path outputFile, Reducer reducer) throws IOException {
        PriorityQueue<StreamNode> pq = new PriorityQueue<>(Comparator.comparing(n -> n.currentPair.key()));
        List<BufferedReader> activeReaders = new ArrayList<>();

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

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

            while (!pq.isEmpty()) {
                StreamNode minNode = pq.poll();
                KeyValuePair pair = minNode.currentPair;

                if (currentGroupingKey == null) {
                    currentGroupingKey = pair.key();
                }

                if (!currentGroupingKey.equals(pair.key())) {
                    executeReduceAndWrite(currentGroupingKey, currentValuesGroup, reducer, writer);

                    currentGroupingKey = pair.key();
                    currentValuesGroup.clear();
                }

                currentValuesGroup.add(pair.value());

                StreamNode nextNode = StreamNode.fromReader(minNode.reader);
                if (nextNode != null) {
                    pq.add(nextNode);
                }
            }

            if (currentGroupingKey != null && !currentValuesGroup.isEmpty()) {
                executeReduceAndWrite(currentGroupingKey, currentValuesGroup, reducer, writer);
            }

        } finally {
            for (BufferedReader reader : activeReaders) {
                try { reader.close(); } catch (IOException ignored) {}
            }
        }
    }

    /**
     * Executes the user-defined Reducer logic and persists the result to the output writer.
     *
     * @param key     The grouping key.
     * @param values  The collection of values associated with the key.
     * @param reducer The Reducer implementation to apply.
     * @param writer  The destination writer for persisting the result.
     * @throws IOException If disk I/O fails during the write process.
     */
    private static void executeReduceAndWrite(String key, List<String> values, Reducer reducer, BufferedWriter writer) throws IOException {
        KeyValuePair reducedResult = reducer.reduce(key, values);
        if (reducedResult != null) {
            writer.write(reducedResult.key() + "\t" + reducedResult.value() + "\n");
        }
    }

    /**
     * Recursively purges a local directory and its entire contents.
     *
     * <p>Cleans up temporary sorted runs and root directories after data has
     * been successfully persisted to shared storage (S3).</p>
     *
     * @param directoryToBeDeleted The target directory path to be purged.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
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
     * Internal data container representing an active file stream during a K-Way merge operation.
     */
    private static class StreamNode {
        /** The current key-value pair extracted from the stream. */
        KeyValuePair currentPair;
        /** The reader associated with the active sorted run file. */
        BufferedReader reader;

        StreamNode(KeyValuePair currentPair, BufferedReader reader) {
            this.currentPair = currentPair;
            this.reader = reader;
        }

        /**
         * Extracts the next record from the reader and encapsulates it in a StreamNode.
         *
         * @param reader The active run file reader.
         * @return A new {@code StreamNode}, or {@code null} if the end of the file is reached.
         * @throws IOException If a disk I/O failure occurs during read.
         */
        static StreamNode fromReader(BufferedReader reader) throws IOException {
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return null;
            }
            int tabIndex = line.indexOf('\t');
            if (tabIndex > 0) {
                return new StreamNode(new KeyValuePair(line.substring(0, tabIndex), line.substring(tabIndex + 1)), reader);
            }
            return null;
        }
    }
}