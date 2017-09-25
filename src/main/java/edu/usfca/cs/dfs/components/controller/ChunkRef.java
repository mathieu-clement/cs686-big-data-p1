package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.structures.ComponentAddress;

import java.util.Set;
import java.util.TreeSet;

/**
 * A ChunkRef (full name: File Chunk Reference) is used by the controller, as an object
 * that refers to a Chunk object stored on a storage node.
 */
public class ChunkRef implements Comparable<ChunkRef> {
    private final String filename;
    private final int sequenceNo;
    private final long size;
    private Set<ComponentAddress> replicaLocations = new TreeSet<>();

    public ChunkRef(String filename, int sequenceNo, long size) {
        this.filename = filename;
        this.sequenceNo = sequenceNo;
        this.size = size;
    }

    public int getNumberOfReplicas() {
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

    public long getSize() {
        return size;
    }
}
