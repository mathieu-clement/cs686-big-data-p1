package edu.usfca.cs.dfs.structures;

import org.junit.jupiter.api.Test;

import static edu.usfca.cs.dfs.structures.Chunk.calculateNumberOfChunks;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ChunkTest {
    @Test
    void testCalculateNumberOfChunks() {
        assertEquals(1, calculateNumberOfChunks(100, 100));
        assertEquals(2, calculateNumberOfChunks(100, 99));
        assertEquals(2, calculateNumberOfChunks(100, 50));
        assertEquals(3, calculateNumberOfChunks(100, 49));
        assertEquals(1, calculateNumberOfChunks(100, 101));

    }

}