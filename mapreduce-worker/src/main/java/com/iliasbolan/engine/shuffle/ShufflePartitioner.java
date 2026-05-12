package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enterprise-grade ShufflePartitioner with O(1) Memory Persistent Streams.
 * <p>
 * <b>Architectural Fix: Zero-Aggregation Write-Through</b><br>
 * This version eliminates the {@link StringBuilder} buffering that caused heap
 * spikes during large 11GB shuffles. Records are streamed directly to the
 * partition-specific {@link BufferedWriter} instances, ensuring the memory
 * footprint remains constant regardless of chunk density or record length.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.2
 * @since 2026-05-12
 */
public class ShufflePartitioner implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ShufflePartitioner.class);
    private final String baseShuffleDir;
    private final String jobId;
    private final String mapTaskId;
    private final int numReducers;
    private final Object[] partitionLocks;

    /** Persistent cache of open writers to prevent LZ4 frame thrashing. */
    private final ConcurrentHashMap<Integer, BufferedWriter> writerCache = new ConcurrentHashMap<>();

    public ShufflePartitioner(String baseShuffleDir, String jobId, String mapTaskId, int numReducers) {
        this.baseShuffleDir = baseShuffleDir;
        this.jobId = jobId;
        this.mapTaskId = mapTaskId;
        this.numReducers = numReducers;

        // Lock stripping per partition to allow concurrent thread access
        this.partitionLocks = new Object[numReducers];
        for (int i = 0; i < numReducers; i++) {
            this.partitionLocks[i] = new Object();
        }
    }

    /**
     * Appends records directly to persistent LZ4 streams without intermediate heap buffering.
     * <p>
     * <b>O(1) Memory Guarantee:</b> By writing records one-by-one to the underlying
     * buffered streams, this method maintains a flat memory profile, preventing OOM
     * crashes on dense data chunks.
     * </p>
     */
    public void appendThreadSafe(List<KeyValuePair> buffer) throws IOException {
        for (KeyValuePair pair : buffer) {
            // Determine partition via bitmasked hash
            int partitionIndex = (pair.key().hashCode() & Integer.MAX_VALUE) % numReducers;

            // Use lock stripping to allow threads to write to different partitions in parallel
            synchronized (partitionLocks[partitionIndex]) {
                BufferedWriter writer = getOrCreateWriter(partitionIndex);

                // PERFORMANCE: Write components individually to avoid String concatenation spikes
                writer.write(pair.key());
                writer.write('\t');
                writer.write(pair.value());
                writer.write('\n');
            }
        }
    }

    private BufferedWriter getOrCreateWriter(int partitionIndex) throws IOException {
        BufferedWriter cachedWriter = writerCache.get(partitionIndex);
        if (cachedWriter != null) return cachedWriter;

        Path partitionDir = Paths.get(baseShuffleDir, jobId, String.valueOf(partitionIndex));
        Files.createDirectories(partitionDir);

        // Use TRUNCATE_EXISTING instead of APPEND. If a task is retried,
        // we must start the LZ4 frame fresh to prevent binary corruption.
        Path filePath = partitionDir.resolve(mapTaskId + ".lz4");
        OutputStream os = Files.newOutputStream(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(os);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(lz4os, StandardCharsets.UTF_8));

        writerCache.put(partitionIndex, writer);
        return writer;
    }

    @Override
    public void close() throws IOException {
        logger.info("Finalizing LZ4 partitions for task: {}", mapTaskId);
        for (BufferedWriter writer : writerCache.values()) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                logger.error("Failed to finalize shuffle stream", e);
            }
        }
        writerCache.clear();
    }
}