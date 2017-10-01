package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Periodically checks if chunks need to be replicated, and if so orders
 * storage nodes to send chunks to other nodes.
 */
public class ChunkReplicationRunnable implements Runnable {

    private final Map<String, SortedSet<Chunk>> chunks;
    private final Set<ComponentAddress> storageNodes;
    private final FileTable fileTable;

    public ChunkReplicationRunnable(Map<String, SortedSet<Chunk>> chunks, Set<ComponentAddress> storageNodes, FileTable fileTable) {
        this.chunks = chunks;
        this.storageNodes = storageNodes;
        this.fileTable = fileTable;
    }

    @Override
    public void run() {
    }
}
