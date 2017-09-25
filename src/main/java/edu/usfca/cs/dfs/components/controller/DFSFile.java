package edu.usfca.cs.dfs.components.controller;

import java.util.Set;
import java.util.TreeSet;

/**
 * Structure used by the controller to manage the files stored in the DFS
 */
public class DFSFile implements Comparable<DFSFile> {
    private final String filename;
    private final long size;
    private final String checksum;

    private final Set<ChunkRef> chunks = new TreeSet<>();

    public DFSFile(String filename, long size, String checksum) {
        this.filename = filename;
        this.size = size;
        this.checksum = checksum;
    }

    public Set<ChunkRef> getChunks() {
        return chunks;
    }

    @Override
    public int compareTo(DFSFile o) {
        return this.filename.compareTo(o.filename);
    }
}
