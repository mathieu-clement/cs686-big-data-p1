package edu.usfca.cs.dfs.components.controller;

import java.util.*;

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

    public int getChunkCount() {
        return this.chunks.size();
    }

    @Override
    public int compareTo(DFSFile o) {
        return this.filename.compareTo(o.filename);
    }

    public String getFilename() {
        return filename;
    }

    public void removeChunk(int sequenceNo) {
        chunks.remove(sequenceNo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DFSFile dfsFile = (DFSFile) o;
        return Objects.equals(filename, dfsFile.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }
}
