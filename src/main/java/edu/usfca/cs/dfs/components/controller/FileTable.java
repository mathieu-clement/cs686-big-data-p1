package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.structures.ComponentAddress;

import java.util.*;

/**
 * The FileTable is a list of all the files and chunks known to the controller.
 * It only contains files and chunks that are currently reachable, i.e. there is at least
 * one replica on one reachable storage node for that chunk.
 *
 */
public class FileTable {

    private final Map<String, DFSFile> files = new HashMap<>();

    /**
     * Returns names of the files in the system, sorted.
     *
     * @return names of the files in the system
     */
    public SortedSet<String> getFilenames() {
        return new TreeSet<>(files.keySet());
    }

    /**
     * Look up a file (and its chunks and replicas) by name
     *
     * @param filename Filename
     * @return file
     */
    public DFSFile getFile(String filename) {
        return files.get(filename);
    }

    /**
     * Returns list of chunks that need to be replicated
     * @return list of chunks that need to be replicated
     */
    public List<ChunkRef> getUnderReplicatedChunks() {
        List<ChunkRef> chunks = new ArrayList<>();
        int minReplicas = DFSProperties.getInstance().getMinReplicas();
        for (DFSFile file : files.values()) {
            for (ChunkRef chunk : file.getChunks()) {
                if (chunk.getReplicaCount() < minReplicas) {
                    chunks.add(chunk);
                }
            }
        }
        return chunks;
    }

    /**
     * Remove replicas from a storage node that has gone offline, which might remove
     * a chunk altogether, and even a file altogether.
     * @param storageNode storage node now offline
     */
    public void onStorageNodeOffline(ComponentAddress storageNode) {
        // Chunks that are modified and that we need to remove later in case
        // there are no more replicas left
        List<ChunkRef> modifiedChunks = new ArrayList<>();

        // Files that are modified and that we need to remove later in case
        // there are no chunks left
        List<DFSFile> modifiedFiles = new ArrayList<>();

        for (DFSFile file : files.values()) {
            for (ChunkRef chunk : file.getChunks()) {
                Set<ComponentAddress> locations = chunk.getReplicaLocations();
                if (locations.contains(storageNode)) {
                    locations.remove(storageNode);
                    modifiedChunks.add(chunk);
                }
            }
        }

        // Remove chunks that have no replicas
        for (ChunkRef chunk : modifiedChunks) {
            if (chunk.getReplicaCount() == 0) {
                DFSFile file = files.get(chunk.getFilename());
                file.removeChunk(chunk.getSequenceNo());
                modifiedFiles.add(file);
            }
        }

        // Remove files that have no chunks
        for (DFSFile file : modifiedFiles) {
            if (file.getChunkCount() == 0) {
                files.remove(file.getFilename());
            }
        }
    }

    /**
     * Announce to the filetable that a certain storage node has got a
     * particular chunk.
     *
     * @param filename    name of whole file originally received from client
     * @param sequenceNo  chunk sequence number
     * @param storageNode storage node that has that chunk
     */
    public void publishChunk(String filename,
                             int sequenceNo,
                             ComponentAddress storageNode) {
        if (!files.containsKey(filename)) {
            files.put(filename, new DFSFile(filename));
        }

        DFSFile file = files.get(filename);
        if (!file.hasChunk(sequenceNo)) {
            file.addChunk(new ChunkRef(filename, sequenceNo));
        }
        Set<ComponentAddress> replicaLocations = file.getChunk(sequenceNo).getReplicaLocations();
        if (!replicaLocations.contains(storageNode)) {
            replicaLocations.add(storageNode);
        }
    }
}
