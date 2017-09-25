package edu.usfca.cs.dfs.components.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Structure used by the controller to manage the files stored in the DFS
 */
public class DFSFile implements Comparable<DFSFile> {
    private final String filename;

    private final Map<Integer, ChunkRef> chunks = new HashMap<>();

    public DFSFile(String filename) {
        this.filename = filename;
    }

    public SortedSet<ChunkRef> getChunks() {
        return new TreeSet<>(chunks.values());
    }

    public ChunkRef getChunk(int sequenceNo) {
        return chunks.get(sequenceNo);
    }

    public void addChunk(ChunkRef chunkRef) {
        this.chunks.put(chunkRef.getSequenceNo(), chunkRef);
    }

    public boolean hasChunk(int sequenceNo) {
        return this.chunks.containsKey(sequenceNo);
    }

    public long getSize() {
        long fileSize = 0;
        for (ChunkRef chunk : chunks.values()) {
            fileSize += chunk.getSize();
        }
        return fileSize;
    }

    @Override
    public int compareTo(DFSFile o) {
        return this.filename.compareTo(o.filename);
    }

    public String getFilename() {
        return filename;
    }
}
