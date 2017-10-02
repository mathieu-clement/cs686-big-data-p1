package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

class MessageProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private final Map<ComponentAddress, MessageFifoQueue> messageQueues;
    private final Set<ComponentAddress> onlineStorageNodes;
    private StorageNodeAddressService storageNodeAddressService;
    private final FileTable fileTable;
    private final Socket socket;

    public MessageProcessor(StorageNodeAddressService storageNodeAddressService, Set<ComponentAddress> onlineStorageNodes, Map<ComponentAddress, MessageFifoQueue> messageQueues, FileTable fileTable, Socket socket) {
        this.storageNodeAddressService = storageNodeAddressService;
        this.onlineStorageNodes = onlineStorageNodes;
        this.messageQueues = messageQueues;
        this.fileTable = fileTable;
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
        removeFromMessageQueue();
    }

    private void removeFromMessageQueue() {
        if (storageNodeAddressService.getStorageNodeAddress() != null) {
            messageQueues.remove(storageNodeAddressService.getStorageNodeAddress());
        }
    }

    private void processGetStorageNodesRequestMsg() throws IOException {
        List<Messages.StorageNode> msgStorageNodeList = new ArrayList<>(onlineStorageNodes.size());
        for (ComponentAddress onlineStorageNode : onlineStorageNodes) {
            msgStorageNodeList.add(Messages.StorageNode.newBuilder()
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
        send(responseMsgWrapper);
    }

    private void send(Messages.MessageWrapper msg) {
        messageQueues.get(storageNodeAddressService.getStorageNodeAddress()).queue(msg);
    }

    private void processHeartbeatMsg(Messages.MessageWrapper msgWrapper) {
        Messages.Heartbeat msg = msgWrapper.getHeartbeatMsg();
        ComponentAddress storageNodeAddress = new ComponentAddress(
                msg.getStorageNodeHost(),
                msg.getStorageNodePort());
        ComponentAddress storageNode = new ComponentAddress(msg.getStorageNodeHost(), msg.getStorageNodePort());

        Map<String, SortedSet<Integer>> fileChunks = toFileChunksMap(msg.getFileChunksList());
        for (Map.Entry<String, SortedSet<Integer>> entry : fileChunks.entrySet()) {
            String filename = entry.getKey();
            SortedSet<Integer> sequenceNos = entry.getValue();
            for (Integer sequenceNo : sequenceNos) {
                fileTable.publishChunk(filename, sequenceNo, storageNode);
            }
        }

        logger.trace("Received heartbeat from " + storageNodeAddress + " with file chunks: " + fileChunks);
        this.storageNodeAddressService.setStorageNodeAddress(storageNodeAddress);
        onlineStorageNodes.add(storageNodeAddress);
        createMessageQueueIfNotExists(storageNodeAddress);
    }

    private void createMessageQueueIfNotExists(ComponentAddress storageNodeAddress) {
        if (!messageQueues.containsKey(storageNodeAddress)) {
            messageQueues.put(storageNodeAddress, new MessageFifoQueue());
        }
    }

    private Map<String, SortedSet<Integer>> toFileChunksMap(List<Messages.FileChunks> pbFileChunks) {
        Map<String, SortedSet<Integer>> result = new HashMap<>();
        for (Messages.FileChunks pbFileChunk : pbFileChunks) {
            String filename = pbFileChunk.getFilename();
            TreeSet<Integer> sequenceNos = new TreeSet<>(pbFileChunk.getSequenceNosList());
            result.put(filename, sequenceNos);
        }
        return result;
    }
}
