package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

class ProcessIncomingMessageRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ProcessIncomingMessageRunnable.class);
    private final Set<ComponentAddress> onlineStorageNodes;
    private final Socket socket;

    public ProcessIncomingMessageRunnable(Set<ComponentAddress> onlineStorageNodes, Socket socket) {
        this.onlineStorageNodes = onlineStorageNodes;
        this.socket = socket;
    }

    @Override
    public void run() {
        while (!socket.isClosed()) {
            try {
                Messages.MessageWrapper msgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());

                if (msgWrapper.hasHeartbeatMsg()) {
                    processHeartbeatMsg(msgWrapper);
                } else if (msgWrapper.hasGetStoragesNodesRequestMsg()) {
                    processGetStorageNodesRequestMsg();
                }
            } catch (IOException e) {
                logger.error("Error reading from socket", e);
            }
        }
    }

    private void processGetStorageNodesRequestMsg() throws IOException {
        List<Messages.GetStorageNodesResponse.StorageNode> msgStorageNodeList = new ArrayList<>(onlineStorageNodes.size());
        for (ComponentAddress onlineStorageNode : onlineStorageNodes) {
            msgStorageNodeList.add(Messages.GetStorageNodesResponse.StorageNode.newBuilder()
                    .setHost(onlineStorageNode.getHost())
                    .setPort(onlineStorageNode.getPort())
                    .build()
            );
        }
        Messages.GetStorageNodesResponse storageNodesResponse = Messages.GetStorageNodesResponse.newBuilder()
                .addAllNodes(msgStorageNodeList)
                .build();
        Messages.MessageWrapper responseMsgWrapper = Messages.MessageWrapper.newBuilder()
                .setGetStorageNodesResponseMsg(storageNodesResponse)
                .build();
        responseMsgWrapper.writeDelimitedTo(socket.getOutputStream());
    }

    private void processHeartbeatMsg(Messages.MessageWrapper msgWrapper) {
        Messages.Heartbeat msg = msgWrapper.getHeartbeatMsg();
        ComponentAddress storageNodeAddress = new ComponentAddress(
                msg.getStorageNodeHost(),
                msg.getStorageNodePort());

        Map<String, SortedSet<Integer>> fileChunks = toFileChunksMap(msg.getFileChunksList());

        logger.trace("Received heartbeat from " + storageNodeAddress + " with file chunks: " + fileChunks);
        onlineStorageNodes.add(storageNodeAddress);
    }

    private Map<String, SortedSet<Integer>> toFileChunksMap(List<Messages.Heartbeat.FileChunks> pbFileChunks) {
        Map<String, SortedSet<Integer>> result = new HashMap<>();
        for (Messages.Heartbeat.FileChunks pbFileChunk : pbFileChunks) {
            String filename = pbFileChunk.getFilename();
            TreeSet<Integer> sequenceNos = new TreeSet<>(pbFileChunk.getSequenceNosList());
            result.put(filename, sequenceNos);
        }
        return result;
    }
}
