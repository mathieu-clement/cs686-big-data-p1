package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.structures.Chunk;

import java.util.Map;
import java.util.SortedSet;

public class ChunkCorruptionMonitor implements Runnable {

    private final Map<String, SortedSet<Chunk>> chunks;

    public ChunkCorruptionMonitor(Map<String, SortedSet<Chunk>> chunks) {
        this.chunks = chunks;
    }

    @Override
    public void run() {

    }
}
