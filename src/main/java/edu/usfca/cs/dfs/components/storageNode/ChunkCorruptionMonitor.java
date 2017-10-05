package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.structures.Chunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class ChunkCorruptionMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ChunkCorruptionMonitor.class);
    private final Map<String, SortedSet<Chunk>> chunkMap;
    private final Lock chunkMapLock;

    public ChunkCorruptionMonitor(Map<String, SortedSet<Chunk>> chunkMap, Lock chunkMapLock) {
        this.chunkMap = chunkMap;
        this.chunkMapLock = chunkMapLock;
    }

    @Override
    public void run() {
        while (true) {
            for (SortedSet<Chunk> chunks : chunkMap.values()) {
                List<Chunk> corruptedChunks = new ArrayList<>();
                for (Chunk chunk : chunks) {
                    if (chunk.isCorrupted()) {
                        corruptedChunks.add(chunk);
                    }
                }
                for (Chunk chunk : corruptedChunks) {
                    File chunkFile = chunk.getChunkLocalPath().toFile();
                    File checksumFile = new File(chunkFile.getAbsolutePath() + ".md5");
                    chunkFile.delete();
                    checksumFile.delete();
                    chunkMapLock.lock();
                    try {
                        // Just delete the chunk from this storage node,
                        // the controller will do the rest.
                        chunks.remove(chunk);
                    } finally {
                        chunkMapLock.unlock();
                    }
                }
            }

            // We might have removed all known chunks of a file
            for (String filename : new HashSet<>(chunkMap.keySet())) {
                if (chunkMap.get(filename).isEmpty()) {
                    chunkMap.remove(filename);
                }
            }

            try {
                Thread.sleep(DFSProperties.getInstance().getCorruptionVerificationPeriod());
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            }
        }
    }
}
