package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.Utils;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Periodically checks if chunks need to be replicated, and if so orders
 * storage nodes to send chunks to other nodes.
 */
public class ChunkReplicationRunnable implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(ChunkReplicationRunnable.class);
    private final Set<ComponentAddress> storageNodes;
    private final Map<ComponentAddress, MessageFifoQueue> messageQueues;
    private final FileTable fileTable;

    public ChunkReplicationRunnable(Set<ComponentAddress> onlineStorageNodes, Map<ComponentAddress, MessageFifoQueue> messageQueues, FileTable fileTable) {
        this.storageNodes = onlineStorageNodes;
        this.messageQueues = messageQueues;
        this.fileTable = fileTable;
    }

    @Override
    public void run() {
        try {
            int checkPeriod = DFSProperties.getInstance().getReplicationCheckPeriod();
            while (true) {
                List<ChunkRef> underReplicatedChunks = fileTable.getUnderReplicatedChunks();
                if (underReplicatedChunks != null && !underReplicatedChunks.isEmpty()) {
                    orderMoreReplicas(underReplicatedChunks);
                }

                Thread.sleep(checkPeriod);
            }
        } catch (InterruptedException e) {
            logger.error("Thread interrupted", e);
        }
    }

    private void orderMoreReplicas(List<ChunkRef> underReplicatedChunks) {
        int minReplicas = DFSProperties.getInstance().getMinReplicas();
        for (ChunkRef chunk : underReplicatedChunks) {
            Set<ComponentAddress> unusedStorageNodes = new HashSet<>(storageNodes);
            unusedStorageNodes.removeAll(chunk.getReplicaLocations());

            if (unusedStorageNodes.isEmpty()) {
                logger.warn("There are not enough nodes online, not even one that could help satisfy the replication strategy.");
                return;
            }
            int missingReplicas = minReplicas - chunk.getReplicaCount();
            Set<ComponentAddress> additionalNodes = Utils.chooseNrandomOrMin(missingReplicas, unusedStorageNodes);
            if (additionalNodes.size() != missingReplicas) {
                logger.warn(chunk + ": " + missingReplicas + " more replica(s) needed, but as of now we can only get " + additionalNodes.size() + " more.");

            }

            for (ComponentAddress additionalNode : additionalNodes) {
                Messages.OrderSendChunk internalMsg = Messages.OrderSendChunk.newBuilder()
                        .setStorageNode(
                                Messages.StorageNode.newBuilder()
                                        .setHost(additionalNode.getHost())
                                        .setPort(additionalNode.getPort())
                                        .build()
                        )
                        .setFileChunk(
                                Messages.FileChunk.newBuilder()
                                        .setFilename(chunk.getFilename())
                                        .setSequenceNo(chunk.getSequenceNo())
                                        .build()
                        )
                        .build();
                Messages.MessageWrapper msg = Messages.MessageWrapper.newBuilder()
                        .setOrderSendChunkMsg(internalMsg)
                        .build();

                ComponentAddress senderNode = Utils.chooseNrandomOrMin(1, chunk.getReplicaLocations()).iterator().next();
                logger.debug("Telling " + senderNode + " to transfer " + chunk + " to " + additionalNode);
                messageQueues.get(senderNode).queue(msg);
            }
        }

    }
}
