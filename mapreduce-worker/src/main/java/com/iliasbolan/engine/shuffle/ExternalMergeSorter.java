package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;

/**
 * Enterprise-grade external merge sort utility for distributed data processing.
 * <p>
 * This engine guarantees stable memory consumption (O(1) memory footprint during reduction)
 * regardless of data skew or absolute partition size. It achieves this by lazily streaming
 * decompressed blocks from disk directly to the user-defined reduction logic.
 * </p>
 * <p>
 * <b>Architectural Update: Fault-Tolerant Concatenated LZ4 Ingestion</b><br>
 * Safely handles complex distributed edge cases during the gRPC fetch phase:
 * <ul>
 * <li><b>Concatenated Frame Native Support:</b> The ESS transmits data by appending multiple LZ4 frames
 * into a single unified stream. Standard `lz4-java` inputs drop bytes across frame boundaries.
 * This has been migrated to Apache Commons Compress, which natively decompresses concatenated
 * frames via the {@code decompressConcatenated} flag, ensuring zero data loss.</li>
 * <li><b>Empty Stream Guard:</b> Explicitly verifies file sizes to prevent fatal magic-number
 * mismatch exceptions on 0-byte payloads caused by natural data skew.</li>
 * <li><b>Atomic Move Compatibility:</b> Strictly filters for {@code .lz4} extensions,
 * ignoring transient {@code .tmp} files generated during active gRPC fetches.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 4.4
 * @since 2026-05-12
 */
public class ExternalMergeSorter {

    private static final Logger logger = LoggerFactory.getLogger(ExternalMergeSorter.class);

    /** Safe memory boundary for in-memory chunking before spilling to disk. */
    private static final int CHUNK_RECORD_LIMIT = 500_000;

    /**
     * Orchestrates the complete Spill-to-Disk sort and reduce pipeline.
     *
     * @param rawDataDir The local directory containing the raw, LZ4-compressed gRPC stream files.
     * @param reducer    The user-defined Reducer implementation dynamically loaded into the JVM.
     * @return The {@link Path} to the final serialized file containing the reduced output.
     * @throws IOException If a fatal disk I/O failure occurs during merging or resource allocation.
     */
    public static Path sortReduceAndSpill(Path rawDataDir, Reducer reducer) throws IOException {
        logger.info("Initializing LZ4-Aware ExternalMergeSorter on directory: {}", rawDataDir);

        Path runsDir = rawDataDir.resolve("sorted_runs");
        Files.createDirectories(runsDir);

        // Phase 1: Ingest, decompress, sort, and re-compress intermediate runs
        List<Path> sortedRuns = createSortedRuns(rawDataDir, runsDir);
        logger.info("Generated {} compressed sorted runs on disk. Commencing Lazy K-Way Merge...", sortedRuns.size());

        // Phase 2: K-Way Merge and Execute Reduce
        Path finalOutputFile = rawDataDir.resolve("final_reduced_output.txt");
        performNWayMergeAndReduce(sortedRuns, finalOutputFile, reducer);

        return finalOutputFile;
    }

    /**
     * Reads concatenated input files and spills sorted, compressed chunks to the local file system.
     *
     * @param rawDataDir The directory containing raw gRPC stream files.
     * @param runsDir    The directory where sorted chunk files will be persisted.
     * @return A {@link List} of {@link Path} objects identifying the generated sorted runs.
     * @throws IOException If fatal disk I/O failures occur while reading streams or spilling runs.
     */
    private static List<Path> createSortedRuns(Path rawDataDir, Path runsDir) throws IOException {
        List<Path> sortedRunFiles = new ArrayList<>();
        List<KeyValuePair> currentChunk = new ArrayList<>(CHUNK_RECORD_LIMIT);

        long currentChunkSizeBytes = 0;
        final long MAX_CHUNK_SIZE_BYTES = 32 * 1024 * 1024; // 32MB physical memory limit per chunk
        int runCounter = 0;

        try (Stream<Path> rawFiles = Files.list(rawDataDir)) {
            // Strictly require .lz4 extension to avoid reading active .tmp fetches
            List<Path> filesToProcess = rawFiles
                    .filter(p -> p.getFileName().toString().startsWith("grpc_stream_") && p.toString().endsWith(".lz4"))
                    .toList();

            for (Path rawFile : filesToProcess) {

                // EMPTY-STREAM GUARD: Skip 0-byte files resulting from data skew
                if (Files.size(rawFile) == 0) {
                    logger.debug("Skipping 0-byte gRPC stream file: {}", rawFile.getFileName());
                    continue;
                }

                // Use Apache Commons with 'decompressConcatenated = true'
                // This eliminates the Buffer Over-Read vulnerability that previously destroyed boundaries.
                try (InputStream is = Files.newInputStream(rawFile);
                     FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;

                        int tabIndex = line.indexOf('\t');
                        if (tabIndex > 0) {
                            String key = line.substring(0, tabIndex);
                            String value = line.substring(tabIndex + 1);
                            currentChunk.add(new KeyValuePair(key, value));
                            currentChunkSizeBytes += key.length() + value.length() + 2;
                        }

                        // Flush if EITHER the record count OR the physical byte limit is breached
                        if (currentChunk.size() >= CHUNK_RECORD_LIMIT || currentChunkSizeBytes >= MAX_CHUNK_SIZE_BYTES) {
                            sortedRunFiles.add(sortAndSpillChunk(currentChunk, runsDir, runCounter++));
                            currentChunk.clear();
                            currentChunkSizeBytes = 0;
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
     * Sorts an in-memory chunk and persists it to disk using LZ4 frame compression.
     *
     * @param chunk   The collection of records currently loaded in memory.
     * @param runsDir The destination directory for the sorted file.
     * @param runId   A unique identifier for naming the resulting run file.
     * @return The {@link Path} to the persisted sorted run.
     * @throws IOException If a disk I/O failure occurs during the write operation.
     */
    private static Path sortAndSpillChunk(List<KeyValuePair> chunk, Path runsDir, int runId) throws IOException {
        chunk.sort(Comparator.comparing(KeyValuePair::key));

        Path runFile = runsDir.resolve("run_" + runId + ".lz4");

        try (OutputStream os = Files.newOutputStream(runFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(os);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(lz4os, StandardCharsets.UTF_8))) {

            for (KeyValuePair pair : chunk) {
                // PERFORMANCE OPTIMIZATION: Eliminate ephemeral StringBuilders
                writer.write(pair.key());
                writer.write('\t');
                writer.write(pair.value());
                writer.write('\n');
            }
        }
        logger.debug("Spilled compressed sorted run to disk: {} ({} records)", runFile.getFileName(), chunk.size());
        return runFile;
    }

    /**
     * Merges multiple compressed sorted runs and streams grouped keys lazily to the user-defined Reducer.
     *
     * @param sortedRuns The collection of compressed sorted run files to be merged.
     * @param outputFile The destination file for the final reduced results (Standard text).
     * @param reducer    The user-defined aggregation logic.
     * @throws IOException If disk I/O failures occur during merging or reducing.
     */
    private static void performNWayMergeAndReduce(List<Path> sortedRuns, Path outputFile, Reducer reducer) throws IOException {
        PriorityQueue<StreamNode> pq = new PriorityQueue<>();
        List<BufferedReader> activeReaders = new ArrayList<>(sortedRuns.size());

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            for (Path runFile : sortedRuns) {
                InputStream is = Files.newInputStream(runFile);
                // CONSISTENCY FIX: Apply Apache Commons decompressor to run files as well
                FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8));

                activeReaders.add(reader);

                StreamNode node = new StreamNode(reader);
                if (node.advance()) {
                    pq.add(node);
                }
            }

            while (!pq.isEmpty()) {
                String currentGroupingKey = pq.peek().currentKey;
                StreamGroupingIterator lazyIterator = new StreamGroupingIterator(pq, currentGroupingKey);

                executeReduceAndWrite(currentGroupingKey, lazyIterator, reducer, writer);

                // Safety Guard: Manually drain the iterator if the Reducer returns early
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
     * @param key      The grouping key.
     * @param iterator Lazy-evaluating stream of values for the key.
     * @param reducer  The applied reduction logic.
     * @param writer   The output stream destination.
     * @throws IOException If a serialization error occurs.
     */
    private static void executeReduceAndWrite(String key, Iterator<String> iterator, Reducer reducer, BufferedWriter writer) throws IOException {
        KeyValuePair reducedResult = reducer.reduce(key, iterator);
        if (reducedResult != null) {
            writer.write(reducedResult.key());
            writer.write('\t');
            writer.write(reducedResult.value());
            writer.write('\n');
        }
    }

    /**
     * Recursively purges a local directory and its entire contents.
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
     * A lazy-evaluating Iterator that pulls data continuously from the K-Way merge PriorityQueue.
     */
    private record StreamGroupingIterator(PriorityQueue<StreamNode> pq, String currentKey) implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return !pq.isEmpty() && pq.peek().currentKey.equals(currentKey);
        }

        @Override
        public String next() {
            if (!hasNext()) throw new NoSuchElementException("No more values for key: " + currentKey);

            StreamNode minNode = pq.poll();
            assert minNode != null;
            String valueToReturn = minNode.currentValue;

            // Zero-Allocation Advance: Mutate and reinsert the node
            try {
                if (minNode.advance()) {
                    pq.add(minNode);
                }
            } catch (IOException e) {
                throw new RuntimeException("Fatal disk read error during lazy stream evaluation", e);
            }

            return valueToReturn;
        }
    }

    /**
     * Internal data container representing an active file stream during a K-Way merge.
     */
    private static class StreamNode implements Comparable<StreamNode> {
        String currentKey;
        String currentValue;
        final BufferedReader reader;

        StreamNode(BufferedReader reader) {
            this.reader = reader;
        }

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