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

class HeartbeatRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatRunnable.class);
    private final ComponentAddress storageNodeAddr;
    private final ComponentAddress controllerAddr;
    private final Map<String, SortedSet<Chunk>> chunks;

    public HeartbeatRunnable(
            ComponentAddress storageNodeAddr,
            ComponentAddress controllerAddr,
            Map<String, SortedSet<Chunk>> chunks) {
        this.storageNodeAddr = storageNodeAddr;
        this.controllerAddr = controllerAddr;
        this.chunks = chunks;
    }

    @Override
    public void run() {
        try {
            Socket socket = controllerAddr.getSocket();
            int heartbeatPeriod = DFSProperties.getInstance().getHeartbeatPeriod();

            while (true) {
                Messages.Heartbeat heartbeatMsg = Messages.Heartbeat.newBuilder()
                        .setStorageNodeHost(storageNodeAddr.getHost())
                        .setStorageNodePort(storageNodeAddr.getPort())
                        .addAllFileChunks(getFileChunks())
                        .build();
                Messages.MessageWrapper msgWrapper =
                        Messages.MessageWrapper.newBuilder()
                                .setHeartbeatMsg(heartbeatMsg)
                                .build();
                logger.trace("Sending heartbeat to controller " + controllerAddr);
                msgWrapper.writeDelimitedTo(socket.getOutputStream());

                Thread.sleep(heartbeatPeriod);
            }
        } catch (IOException e) {
            logger.error("Could not create socket for heartbeat", e);
        } catch (InterruptedException e) {
            logger.warn("Couldn't sleep properly", e);
        }
    }

    private Collection<Messages.Heartbeat.FileChunks> getFileChunks() {
        Set<Messages.Heartbeat.FileChunks> result = new HashSet<>();
        for (Map.Entry<String, SortedSet<Chunk>> entry : chunks.entrySet()) {
            String filename = entry.getKey();
            ArrayList<Integer> sequenceNos = new ArrayList<>(entry.getValue().size());

            for (Chunk chunk : entry.getValue()) {
                sequenceNos.add(chunk.getSequenceNo());
            }

            Messages.Heartbeat.FileChunks fileChunksMsg = Messages.Heartbeat.FileChunks.newBuilder()
                    .setFilename(filename)
                    .addAllSequenceNos(sequenceNos)
                    .build();
            result.add(fileChunksMsg);
        }
        return result;
    }
}
