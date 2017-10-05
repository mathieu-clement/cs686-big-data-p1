package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

class MessageProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static final Random random = new Random();
    private final Map<ComponentAddress, MessageFifoQueue> messageQueues;
    private final Set<ComponentAddress> onlineStorageNodes;
    private final Map<ComponentAddress, Date> heartbeats;
    private StorageNodeAddressService storageNodeAddressService;
    private final FileTable fileTable;
    private final Socket socket;

    public MessageProcessor(StorageNodeAddressService storageNodeAddressService, Set<ComponentAddress> onlineStorageNodes, Map<ComponentAddress, Date> heartbeats, Map<ComponentAddress, MessageFifoQueue> messageQueues, FileTable fileTable, Socket socket) {
        this.storageNodeAddressService = storageNodeAddressService;
        this.onlineStorageNodes = onlineStorageNodes;
        this.heartbeats = heartbeats;
        this.messageQueues = messageQueues;
        this.fileTable = fileTable;
        this.socket = socket;
    }

    @Override
    public void run() {
        int nullMessageCount = 0;
        while (socket.isConnected()) {
            try {
                Messages.MessageWrapper msgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());

                if (msgWrapper == null) {
                    logger.trace("Incoming null message from " + socket.getRemoteSocketAddress());
                    nullMessageCount++;
                    if (nullMessageCount == 50) {
                        logger.error("Too many null messages in a row from " + socket.getRemoteSocketAddress() + ". Closing socket.");
                        socket.close();
                        return;
                    } else {
                        continue;
                    }
                }

                if (msgWrapper.hasHeartbeatMsg()) {
                    processHeartbeatMsg(msgWrapper);
                } else if (msgWrapper.hasGetStoragesNodesRequestMsg()) {
                    logger.trace("Incoming get storage nodes request message");
                    processGetStorageNodesRequestMsg(socket);
                } else if (msgWrapper.hasDownloadFileMsg()) {
                    logger.trace("Incoming download file message");
                    processDownloadFileMsg(socket, msgWrapper);
                } else if (msgWrapper.hasGetFilesRequestMsg()) {
                    logger.trace("Incoming get files request message");
                    processGetFilesRequestMsg(socket);
                } else if (msgWrapper.hasGetFreeSpaceRequestMsg()) {
                    logger.trace("Incoming get free space message");
                    processGetFreeSpaceRequestMsg(socket);
                } else if (msgWrapper.hasChunkCorruptedMsg()) {
                    logger.trace("Incoming chunk corrupted message");
                    processChunkCorruptedMsg(msgWrapper);
                }
            } catch (IOException e) {
                logger.error("Error reading from socket", e);
            }
        }
        removeMessageQueue();
    }

    private void processChunkCorruptedMsg(Messages.MessageWrapper msgWrapper) {
        Messages.ChunkCorrupted msg = msgWrapper.getChunkCorruptedMsg();
        String filename = msg.getFilename();
        int sequenceNo = msg.getSequenceNo();
        String storageNodeHost = msg.getStorageNode().getHost();
        int storageNodePort = msg.getStorageNode().getPort();
        ComponentAddress storageNode = new ComponentAddress(storageNodeHost, storageNodePort);
        logger.warn("Received notice of corrupted file " + filename + " chunk #" + sequenceNo + " on " + storageNode);
        fileTable.onChunkCorrupted(filename, sequenceNo, storageNode);
    }

    private void processGetFreeSpaceRequestMsg(Socket socket) throws IOException {
        long globalFreeSpace = 0L;

        List<Future<Long>> tasks = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (ComponentAddress storageNode : onlineStorageNodes) {
                GetFreeSpaceTask task = new GetFreeSpaceTask(storageNode);
                tasks.add(executor.submit(task));
            }

            for (Future<Long> task : tasks) {
                try {
                    Long size = task.get();
                    globalFreeSpace += size;
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Free space information not complete", e);
                }
            }

            Messages.MessageWrapper.newBuilder()
                    .setGetFreeSpaceResponseMsg(
                            Messages.GetFreeSpaceResponse.newBuilder()
                                    .setFreeSpace(globalFreeSpace)
                                    .build()
                    )
                    .build()
                    .writeDelimitedTo(socket.getOutputStream());
        } finally {
            try {
                logger.trace("Attempting to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            } finally {
                if (!executor.isTerminated()) {
                    logger.error("Some tasks didn't finish.");
                }
                executor.shutdownNow();
                logger.trace("ExecutorService shutdown finished.");
            }
        }
    }

    private static class GetFreeSpaceTask implements Callable<Long> {

        private final ComponentAddress storageNode;

        private GetFreeSpaceTask(ComponentAddress storageNode) {
            this.storageNode = storageNode;
        }

        @Override
        public Long call() throws Exception {
            long freeSpace;
            Socket socket = storageNode.getSocket();

            Messages.MessageWrapper.newBuilder()
                    .setGetFreeSpaceRequestMsg(Messages.GetFreeSpaceRequest.newBuilder().build())
                    .build()
                    .writeDelimitedTo(socket.getOutputStream());

            Messages.MessageWrapper responseMsgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
            if (!responseMsgWrapper.hasGetFreeSpaceResponseMsg()) {
                throw new IllegalStateException("Expected get free space response message, but got: " + responseMsgWrapper);
            }
            freeSpace = responseMsgWrapper.getGetFreeSpaceResponseMsg().getFreeSpace();

            socket.close();

            return freeSpace;
        }
    }

    private void processGetFilesRequestMsg(Socket socket) throws IOException {
        Map<ComponentAddress, Messages.StorageNode> storageNodeMap = new HashMap<>();
        for (ComponentAddress address : onlineStorageNodes) {
            storageNodeMap.put(address,
                    Messages.StorageNode.newBuilder()
                            .setHost(address.getHost())
                            .setPort(address.getPort())
                            .build());
        }

        List<Messages.DownloadFileResponse> downloadFileResponseMessages = new ArrayList<>();
        for (String filename : fileTable.getFilenames()) {
            DFSFile file = fileTable.getFile(filename);

            List<Messages.DownloadFileResponse.ChunkLocation> chunkLocations = new ArrayList<>();
            for (ChunkRef chunk : file.getChunks()) {
                List<Messages.StorageNode> thisMsgStorageNodes = new ArrayList<>();
                for (ComponentAddress address : chunk.getReplicaLocations()) {
                    thisMsgStorageNodes.add(storageNodeMap.get(address));
                }
                chunkLocations.add(
                        Messages.DownloadFileResponse.ChunkLocation.newBuilder()
                                .setSequenceNo(chunk.getSequenceNo())
                                .addAllStorageNodes(thisMsgStorageNodes)
                                .build()
                );
            }

            downloadFileResponseMessages.add(
                    Messages.DownloadFileResponse.newBuilder()
                            .setFilename(filename)
                            .addAllChunkLocations(chunkLocations)
                            .build()
            );
        }

        Messages.MessageWrapper.newBuilder()
                .setGetFilesResponseMsg(
                        Messages.GetFilesResponse.newBuilder()
                                .addAllFiles(downloadFileResponseMessages)
                                .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream());
    }

    private void processDownloadFileMsg(Socket socket, Messages.MessageWrapper msgWrapper) throws IOException {
        Messages.DownloadFile msg = msgWrapper.getDownloadFileMsg();
        String fileName = msg.getFileName();
        DFSFile file = fileTable.getFile(fileName);

        if (file == null) {
            Messages.MessageWrapper responseMsg = Messages.MessageWrapper.newBuilder()
                    .setErrorMsg(Messages.Error.newBuilder().setText("Error: File not found").build())
                    .build();
            responseMsg.writeDelimitedTo(socket.getOutputStream());
            return;
        }

        SortedSet<ChunkRef> chunks = file.getChunks();

        Messages.DownloadFileResponse.Builder downloadFileResponseBuilder = Messages.DownloadFileResponse.newBuilder();
        downloadFileResponseBuilder.setFilename(fileName);

        for (ChunkRef chunk : chunks) {
            Messages.DownloadFileResponse.ChunkLocation.Builder chunkLocationBuilder = Messages.DownloadFileResponse.ChunkLocation.newBuilder()
                    .setSequenceNo(chunk.getSequenceNo());
            for (ComponentAddress storageNode : chunk.getReplicaLocations()) {
                chunkLocationBuilder.addStorageNodes(Messages.StorageNode.newBuilder()
                        .setHost(storageNode.getHost())
                        .setPort(storageNode.getPort())
                        .build());
            }
            downloadFileResponseBuilder.addChunkLocations(chunkLocationBuilder.build());
        }

        Messages.DownloadFileResponse internalMsg = downloadFileResponseBuilder.build();
        Messages.MessageWrapper outMsgWrapper = Messages.MessageWrapper.newBuilder()
                .setDownloadFileResponseMsg(internalMsg)
                .build();
        logger.debug("Telling client where " + fileName + " parts are");
        outMsgWrapper.writeDelimitedTo(socket.getOutputStream());
    }

    private void removeMessageQueue() {
        if (storageNodeAddressService.getStorageNodeAddress() != null) {
            messageQueues.remove(storageNodeAddressService.getStorageNodeAddress());
        }
    }

    private void processGetStorageNodesRequestMsg(Socket socket) throws IOException {
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
        responseMsgWrapper.writeDelimitedTo(socket.getOutputStream());
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

        // Remember that we have seen this heartbeat, to detect missing hearbeats later.
        heartbeats.put(storageNode, new Date());

        Map<String, SortedSet<Integer>> fileChunks = toFileChunksMap(msg.getFileChunksList());
        for (Map.Entry<String, SortedSet<Integer>> entry : fileChunks.entrySet()) {
            String filename = entry.getKey();
            SortedSet<Integer> sequenceNos = entry.getValue();
            for (Integer sequenceNo : sequenceNos) {
                fileTable.publishChunk(filename, sequenceNo, storageNode);
            }
        }

        logger.debug("Received heartbeat from " + storageNodeAddress + " with file chunks: " + fileChunks);
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
