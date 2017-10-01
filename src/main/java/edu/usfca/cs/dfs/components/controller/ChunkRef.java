package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.structures.ComponentAddress;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A ChunkRef (full name: File Chunk Reference) is used by the controller, as an object
 * that refers to a Chunk object stored on a storage node.
 */
public class ChunkRef implements Comparable<ChunkRef> {
    private final String filename;
    private final int sequenceNo;
    private Set<ComponentAddress> replicaLocations = new TreeSet<>();

    public ChunkRef(String filename, int sequenceNo) {
        this.filename = filename;
        this.sequenceNo = sequenceNo;
    }

    public int getReplicaCount() {
        return replicaLocations.size();
    }

    public Set<ComponentAddress> getReplicaLocations() {
        return replicaLocations;
    }

    @Override
    public int compareTo(ChunkRef o) {
        if (!this.filename.equals(o.filename)) {
            return this.filename.compareTo(o.filename);
        }
        return Integer.compare(this.sequenceNo, o.sequenceNo);
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkRef chunkRef = (ChunkRef) o;
        return sequenceNo == chunkRef.sequenceNo &&
                Objects.equals(filename, chunkRef.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, sequenceNo);
    }

    public int getSequenceNo() {
        return sequenceNo;
    }
}
