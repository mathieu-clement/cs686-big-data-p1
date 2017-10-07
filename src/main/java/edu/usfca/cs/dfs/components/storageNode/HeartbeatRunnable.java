package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

class HeartbeatRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatRunnable.class);
    private final ComponentAddress storageNodeAddr;
    private final ComponentAddress controllerAddr;
    private final Map<String, SortedSet<Chunk>> chunks;
    private Map<String, SortedSet<Chunk>> lastChunks = new HashMap<>();
    private final Lock chunksLock;

    public HeartbeatRunnable(
            ComponentAddress storageNodeAddr,
            ComponentAddress controllerAddr,
            Map<String, SortedSet<Chunk>> chunks, Lock chunksLock) {
        this.storageNodeAddr = storageNodeAddr;
        this.controllerAddr = controllerAddr;
        this.chunks = chunks;
        this.chunksLock = chunksLock;
    }

    @Override
    public void run() {
        int heartbeatPeriod = DFSProperties.getInstance().getHeartbeatPeriod();

        while (true) {
            try {
                Socket socket = controllerAddr.getSocket();
                logger.debug("Connected to controller");

                while (true) {
                    Messages.Heartbeat heartbeatMsg = Messages.Heartbeat.newBuilder()
                            .setStorageNodeHost(storageNodeAddr.getHost())
                            .setStorageNodePort(storageNodeAddr.getPort())
                            .addAllFileChunks(getNewFileChunks())
                            .build();
                    Messages.MessageWrapper msgWrapper =
                            Messages.MessageWrapper.newBuilder()
                                    .setHeartbeatMsg(heartbeatMsg)
                                    .build();
                    logger.debug("Sending heartbeat to controller " + controllerAddr);
                    msgWrapper.writeDelimitedTo(socket.getOutputStream());

                    Thread.sleep(heartbeatPeriod);
                }
            } catch (IOException e) {
                logger.error("Could not create socket for heartbeat", e);
            } catch (InterruptedException e) {
                logger.warn("Couldn't sleep properly", e);
            }

            // Retry if socket closed or writing to socket failed
            try {
                Thread.sleep(heartbeatPeriod);
            } catch (InterruptedException e) {
                logger.error("Interrupted while sleeping", heartbeatPeriod);
            }
        }
    }

    static Set<Messages.FileChunks> toFileChunksMessages(Map<String, SortedSet<Chunk>> newChunks) {
        Set<Messages.FileChunks> result = new LinkedHashSet<>();
        for (Map.Entry<String, SortedSet<Chunk>> entry : newChunks.entrySet()) {
            String filename = entry.getKey();
            ArrayList<Integer> sequenceNos = new ArrayList<>(entry.getValue().size());

            for (Chunk chunk : entry.getValue()) {
                sequenceNos.add(chunk.getSequenceNo());
            }

            Messages.FileChunks fileChunksMsg = Messages.FileChunks.newBuilder()
                    .setFilename(filename)
                    .addAllSequenceNos(sequenceNos)
                    .build();
            result.add(fileChunksMsg);
        }
        return result;
    }

    private Collection<Messages.FileChunks> getNewFileChunks() {
        chunksLock.lock();
        try {
            Map<String, SortedSet<Chunk>> newChunks = getDiff(lastChunks, chunks);
            Set<Messages.FileChunks> result = toFileChunksMessages(newChunks);
            lastChunks = cloneChunkMap(); // TODO Only update if HeartbeatAck is received from server
            return result;
        } finally {
            chunksLock.unlock();
        }
    }

    // Clones chunks, by having a new copy of:
    // - the map
    // - the values (sets)
    // but not by cloning the chunks themselves
    private Map<String, SortedSet<Chunk>> cloneChunkMap() {
        Map<String, SortedSet<Chunk>> copy = new HashMap<>();
        for (Map.Entry<String, SortedSet<Chunk>> entry : lastChunks.entrySet()) {
            copy.put(entry.getKey(), new TreeSet<Chunk>(entry.getValue()));
        }
        return copy;
    }

    // Return a map of chunks, with only the stuff that is new
    private Map<String, SortedSet<Chunk>> getDiff(Map<String, SortedSet<Chunk>> oldChunkMap, Map<String, SortedSet<Chunk>> newChunkMap) {
        Map<String, SortedSet<Chunk>> diff = new HashMap<>();
        for (Map.Entry<String, SortedSet<Chunk>> entry : newChunkMap.entrySet()) {
            if (!oldChunkMap.containsKey(entry.getKey())) {
                diff.put(entry.getKey(), entry.getValue());
            } else {
                SortedSet<Chunk> oldChunks = oldChunkMap.get(entry.getKey());
                SortedSet<Chunk> newChunks = new TreeSet<>();
                for (Chunk chunk : entry.getValue()) {
                    if (!oldChunks.contains(chunk)) {
                        newChunks.add(chunk);
                    }
                }
                newChunkMap.put(entry.getKey(), newChunks);
            }
        }
        return diff;
    }

}
