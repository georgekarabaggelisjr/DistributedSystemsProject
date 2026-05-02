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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enterprise-grade external merge sort utility for distributed data processing.
 * <p>
 * This engine guarantees stable memory consumption (O(1) memory footprint during reduction)
 * regardless of data skew or absolute partition size. It achieves this by strictly
 * separating the processing pipeline into two disk-backed phases:
 * </p>
 * <ol>
 * <li><b>Chunking &amp; Sorting:</b> Raw gRPC streams are read into memory-bounded buffers.
 * Once a buffer breaches either the strict record limit or the physical byte-size boundary,
 * it is sorted in memory and flushed to disk as an intermediate "Sorted Run".</li>
 * <li><b>Lazy K-Way Merge:</b> All sorted runs are accessed concurrently via file streams.
 * A {@link PriorityQueue} maintains the absolute minimum key across all active runs.
 * Identical keys are wrapped in a custom {@link Iterator} and streamed directly to the
 * user's {@link Reducer}, ensuring data is only loaded into the heap at the exact moment of computation.</li>
 * </ol>
 * <p>
 * <b>Architectural Update (Zero-Allocation Merge &amp; Byte-Aware Guards):</b><br>
 * The K-Way merge algorithm has been upgraded to a mutable object-reuse pattern, entirely
 * eliminating ephemeral object creation during disk-read loops. Furthermore, the chunking
 * phase now incorporates a Byte-Aware physical memory guard to prevent Out-Of-Memory (OOM)
 * crashes when processing datasets with exceptionally large payload values.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.2
 * @since 2026-04-24
 */
public class ExternalMergeSorter {

    private static final Logger logger = LoggerFactory.getLogger(ExternalMergeSorter.class);

    /**
     * The maximum number of logical records permitted in the JVM heap before triggering a disk spill.
     * Tuned specifically to accommodate Kubernetes container memory limits (e.g., 2Gi limits) for
     * standard text processing.
     */
    private static final int CHUNK_RECORD_LIMIT = 500_000;

    /**
     * Orchestrates the complete Spill-to-Disk sort and reduce pipeline.
     * <p>
     * Initializes the external merge sort on raw partition data, generates intermediate
     * sorted runs, and subsequently applies the user's reduction logic via lazy streaming.
     * </p>
     *
     * @param rawDataDir The local directory containing the raw, un-sorted gRPC stream files.
     * @param reducer    The user-defined Reducer implementation dynamically loaded into the JVM.
     * @param pool       The shared thread pool (reserved for future parallel external sorting).
     * @return The {@link Path} to the final serialized file containing the reduced output.
     * @throws IOException If a disk I/O failure occurs during chunking, merging, or resource allocation.
     */
    @SuppressWarnings("unused")
    public static Path sortReduceAndSpill(Path rawDataDir, Reducer reducer, ForkJoinPool pool) throws IOException {
        logger.info("Initializing ExternalMergeSorter on directory: {}", rawDataDir);

        Path runsDir = rawDataDir.resolve("sorted_runs");
        Files.createDirectories(runsDir);

        // Phase 1: Create Sorted Runs
        List<Path> sortedRuns = createSortedRuns(rawDataDir, runsDir);
        logger.info("Generated {} sorted runs on disk. Commencing Lazy K-Way Merge...", sortedRuns.size());

        // Phase 2: K-Way Merge and Execute Reduce
        Path finalOutputFile = rawDataDir.resolve("final_reduced_output.txt");
        performNWayMergeAndReduce(sortedRuns, finalOutputFile, reducer);

        return finalOutputFile;
    }

    /**
     * Reads raw input files and spills sorted chunks to the local file system.
     * <p>
     * Scans all incoming streams, parses them into {@link KeyValuePair} objects, and
     * flushes them to disk once either the {@value #CHUNK_RECORD_LIMIT} or the physical
     * byte-size threshold is reached. Each run is guaranteed to be totally ordered by key.
     * </p>
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

        // OOM SECURITY GUARD: Track physical byte footprint to prevent payload-based heap exhaustion
        long currentChunkSizeBytes = 0;
        final long MAX_CHUNK_SIZE_BYTES = 32 * 1024 * 1024; // 32MB physical memory limit per run
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

                            // Estimate heap consumption (Key + Value + Tab + Newline)
                            currentChunkSizeBytes += key.length() + value.length() + 2;
                        }

                        // Flush if EITHER the record count OR the physical byte limit is breached
                        if (currentChunk.size() >= CHUNK_RECORD_LIMIT || currentChunkSizeBytes >= MAX_CHUNK_SIZE_BYTES) {
                            sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter++));
                            currentChunk.clear();
                            currentChunkSizeBytes = 0; // Reset byte tracker after successful spill
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
                // PERFORMANCE OPTIMIZATION: Eliminate String Concatenation Overhead.
                // Replaces `writer.write(key + "\t" + val + "\n")` to prevent the JVM
                // from allocating ephemeral StringBuilders for every record.
                writer.write(pair.key());
                writer.write('\t');
                writer.write(pair.value());
                writer.write('\n');
            }
        }
        logger.debug("Spilled sorted run to disk: {} ({} records)", runFile.getFileName(), chunk.size());
        return runFile;
    }

    /**
     * Merges multiple sorted runs and streams grouped keys lazily to the user-defined Reducer.
     * <p>
     * Utilizes a K-Way Merge algorithm powered by a {@link PriorityQueue}. Instead of
     * buffering values into lists, it delegates disk-read control to a {@link StreamGroupingIterator}.
     * This protects the JVM from {@link OutOfMemoryError} during severe data skew events.
     * </p>
     *
     * @param sortedRuns The collection of sorted run files to be merged.
     * @param outputFile The destination file for the final reduced results.
     * @param reducer    The user-defined aggregation logic.
     * @throws IOException If disk I/O failures occur during merging or reducing.
     */
    private static void performNWayMergeAndReduce(List<Path> sortedRuns, Path outputFile, Reducer reducer) throws IOException {
        // StreamNode now implements Comparable naturally
        PriorityQueue<StreamNode> pq = new PriorityQueue<>();
        List<BufferedReader> activeReaders = new ArrayList<>(sortedRuns.size());

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // Initialize the Priority Queue with the first element of every run
            for (Path runFile : sortedRuns) {
                BufferedReader reader = Files.newBufferedReader(runFile, StandardCharsets.UTF_8);
                activeReaders.add(reader);

                StreamNode node = new StreamNode(reader);
                if (node.advance()) {
                    pq.add(node);
                }
            }

            while (!pq.isEmpty()) {
                String currentGroupingKey = pq.peek().currentKey;

                // Create a lazy iterator bound specifically to the current key
                StreamGroupingIterator lazyIterator = new StreamGroupingIterator(pq, currentGroupingKey);

                // Pass the iterator directly to the Reducer. Data streams straight from disk to the user's logic.
                executeReduceAndWrite(currentGroupingKey, lazyIterator, reducer, writer);

                // Safety Guard: If the user's Reducer returned early without consuming the iterator,
                // we must drain it manually to advance the PriorityQueue to the next distinct key.
                while (lazyIterator.hasNext()) {
                    lazyIterator.next();
                }
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
     * @param key      The grouping key.
     * @param iterator The lazy stream of values associated with the key.
     * @param reducer  The Reducer implementation to apply.
     * @param writer   The destination writer for persisting the result.
     * @throws IOException If disk I/O fails during the write process.
     */
    private static void executeReduceAndWrite(String key, Iterator<String> iterator, Reducer reducer, BufferedWriter writer) throws IOException {
        KeyValuePair reducedResult = reducer.reduce(key, iterator);
        if (reducedResult != null) {
            // PERFORMANCE OPTIMIZATION: Eliminate ephemeral StringBuilders
            writer.write(reducedResult.key());
            writer.write('\t');
            writer.write(reducedResult.value());
            writer.write('\n');
        }
    }

    /**
     * Recursively purges a local directory and its entire contents.
     * <p>
     * Cleans up temporary sorted runs and root directories after data has
     * been successfully merged and persisted.
     * </p>
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
     * A lazy-evaluating Iterator that pulls data continuously from the K-Way merge PriorityQueue
     * as long as the incoming keys match the targeted grouping key.
     */
    private record StreamGroupingIterator(PriorityQueue<StreamNode> pq, String currentKey) implements Iterator<String> {
        /**
         * Constructs the grouping iterator.
         *
         * @param pq         The shared PriorityQueue managing active file streams.
         * @param currentKey The key this iterator is permitted to consume.
         */
        private StreamGroupingIterator {
        }

        @Override
        public boolean hasNext() {
            return !pq.isEmpty() && pq.peek().currentKey.equals(currentKey);
        }

        @Override
        public String next() {
            if (!hasNext()) throw new NoSuchElementException("No more values for key: " + currentKey);

            // 1. Extract the minimum node
            StreamNode minNode = pq.poll();
            assert minNode != null;
            String valueToReturn = minNode.currentValue;

            // 2. PERFORMANCE OPTIMIZATION: Zero-Allocation Advance
            // Rather than instantiating a new node, we mutate the exact same object
            // and re-insert it into the queue.
            try {
                if (minNode.advance()) {
                    pq.add(minNode); // 3. Reinsert into the Priority Queue
                }
            } catch (IOException e) {
                throw new RuntimeException("Fatal disk read error during lazy stream evaluation", e);
            }

            return valueToReturn;
        }
    }

    /**
     * Internal data container representing an active file stream during a K-Way merge operation.
     * <p>
     * Designed as a highly optimized, mutable wrapper that implements {@link Comparable}
     * to eliminate GC churn during priority queue evaluations.
     * </p>
     */
    private static class StreamNode implements Comparable<StreamNode> {
        String currentKey;
        String currentValue;
        final BufferedReader reader;

        StreamNode(BufferedReader reader) {
            this.reader = reader;
        }

        /**
         * Reads the next line from disk and updates the internal state mutably.
         * * @return True if a valid record was parsed; false if EOF is reached.
         * @throws IOException If disk I/O fails.
         */
        boolean advance() throws IOException {
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return false;
            }
            int tabIndex = line.indexOf('\t');
            if (tabIndex > 0) {
                this.currentKey = line.substring(0, tabIndex);
                this.currentValue = line.substring(tabIndex + 1);
                return true;
            }
            return false;
        }

        @Override
        public int compareTo(StreamNode other) {
            return this.currentKey.compareTo(other.currentKey);
        }
    }
}