package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.DFSProperties;

import java.util.*;

/**
 * The FileTable is a list of all the files and chunks known to the controller.
 * It only contains files and chunks that are currently reachable, i.e. there is at least
 * one replica on one reachable storage node for that chunk.
 */
public class FileTable {
    // What kind of information is in the file table?
    /*
    Storage nodes
    Files (incl. total size)
    Chunks (incl. size and replica location and count)
     */

    // Supported operations
    /*
        List all files by name -> getFilenames()
        List chunk sizes and locations and number of replica -> getFile()
        List all chunks that need to be replicated, incl. current location of replicas
        TODO: Remove all replicas from a storage node that became offline
        TODO: Add all replicas from a storage that became online
     */

    private final Map<String, DFSFile> files = new HashMap<>();

    public SortedSet<String> getFilenames() {
        return new TreeSet<>(files.keySet());
    }

    public DFSFile getFile(String filename) {
        return files.get(filename);
    }

    public List<ChunkRef> getUnderReplicatedChunks() {
        List<ChunkRef> chunks = new ArrayList<>();
        int minReplicas = DFSProperties.getInstance().getMinReplicas();
        for (DFSFile file : files.values()) {
            for (ChunkRef chunk : file.getChunks()) {
                if (chunk.getNumberOfReplicas() < minReplicas) {
                    chunks.add(chunk);
                }
            }
        }
        return chunks;
    }
}
