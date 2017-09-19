package edu.usfca.cs.dfs.structures;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static edu.usfca.cs.dfs.structures.Chunk.calculateLastChunkSize;
import static edu.usfca.cs.dfs.structures.Chunk.calculateNumberOfChunks;
import static edu.usfca.cs.dfs.structures.Chunk.createChunksFromFile;
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


    @Test
    void testCalculateLastChunkSize() {
        assertEquals(100, calculateLastChunkSize(1, 100, 100));
        assertEquals(1, calculateLastChunkSize(2, 99, 100));
        assertEquals(50, calculateLastChunkSize(2, 50, 100));
        assertEquals(2, calculateLastChunkSize(3, 49, 100));
        assertEquals(100, calculateLastChunkSize(1, 101, 100));
    }


    @Test
    void testCreateChunksFromFile() throws IOException {
        createChunksFromFile("/tmp/input_file", 8 /*  bytes */, "/tmp/chunks");
    }
}