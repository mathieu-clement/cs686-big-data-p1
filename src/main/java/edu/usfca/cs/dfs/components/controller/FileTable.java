package edu.usfca.cs.dfs.components.controller;

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
        TODO: List all files
        TODO: List chunk sizes and locations and number of replicas for each file
        TODO: List all chunks that need to be replicated, incl. current location of replicas
        TODO: Remove all replicas from a storage node that became offline
        TODO: Add all replicas from a storage that became online
     */
}
