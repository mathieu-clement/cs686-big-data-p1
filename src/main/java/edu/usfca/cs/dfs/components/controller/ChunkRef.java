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
    private Set<ComponentAddress> replicaLocations = new TreeSet<>();

    public ChunkRef(String filename, int sequenceNo) {
        this.filename = filename;
        this.sequenceNo = sequenceNo;
    }

    public Set<ComponentAddress> getReplicaLocations() {
        return replicaLocations;
    }

    @Override
    public int compareTo(ChunkRef o) {
        return Integer.compare(this.sequenceNo, o.sequenceNo);
    }
}
