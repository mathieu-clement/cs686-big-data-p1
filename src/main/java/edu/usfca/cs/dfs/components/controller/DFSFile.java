package edu.usfca.cs.dfs.components.controller;

import java.util.Set;
import java.util.TreeSet;

/**
 * Structure used by the controller to manage the files stored in the DFS
 */
public class DFSFile implements Comparable<DFSFile> {
    private final String filename;

    private final Set<ChunkRef> chunks = new TreeSet<>();

    public DFSFile(String filename) {
        this.filename = filename;
    }

    public Set<ChunkRef> getChunks() {
        return chunks;
    }

    public long getSize() {
        long fileSize = 0;
        for (ChunkRef chunk : chunks) {
            fileSize += chunk.getSize();
        }
        return fileSize;
    }

    @Override
    public int compareTo(DFSFile o) {
        return this.filename.compareTo(o.filename);
    }
}
