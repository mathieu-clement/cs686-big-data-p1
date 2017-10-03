package edu.usfca.cs.dfs.components.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.Utils;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class Client {

    private static final Random random = new Random();

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static Semaphore storageNodeAddressesAvailableSema = new Semaphore(0);

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Client controller-host controller-port fileToSend");
            printHelp();
            System.exit(1);
        }

        ComponentAddress controllerAddr = new ComponentAddress(args[0], Integer.parseInt(args[1]));

        String command = args[2].toLowerCase();

        switch (command) {
            case "list-storage-nodes":
                listStorageNodes(controllerAddr);
                break;

            case "upload-file":
                sendFile(controllerAddr, args[3]);
                break;

            case "download-file":
                downloadFile(controllerAddr, args[3]);
                break;

            case "delete-file":
                throw new UnsupportedOperationException("Not implemented yet.");
                // break

            default:
                printHelp();
                System.exit(1);
        }
    }

    private static void listStorageNodes(ComponentAddress controllerAddr) throws IOException {
        List<ComponentAddress> storageNodes = GetStorageNodeListRunnable.fetchStorageNodes(controllerAddr);
        if (storageNodes.isEmpty()) {
            System.out.println("No storage nodes found.");
            return;
        }

        String header = String.format("%-40s %5s", "Host", "Port");
        System.out.println(header);
        for (int i = 0; i < header.length(); ++i) {
            System.out.print("-");
        }
        System.out.println();

        for (ComponentAddress storageNode : storageNodes) {
            System.out.println(String.format("%-40s %5d", storageNode.getHost(), storageNode.getPort()));
        }
    }

    private static void downloadFile(ComponentAddress controllerAddr, String filename) throws IOException, ExecutionException, InterruptedException {
        Messages.MessageWrapper msg = Messages.MessageWrapper.newBuilder()
                .setDownloadFileMsg(
                        Messages.DownloadFile.newBuilder()
                                .setFileName(filename)
                                .build()
                )
                .build();
        Socket controllerSocket = controllerAddr.getSocket();
        logger.info("Asking controller " + controllerAddr + " about file " + filename);
        msg.writeDelimitedTo(controllerSocket.getOutputStream());

        Messages.MessageWrapper msgWrapper = Messages.MessageWrapper.parseDelimitedFrom(controllerSocket.getInputStream());
        if (!msgWrapper.hasDownloadFileResponseMsg()) {
            throw new IllegalStateException("Controller is supposed to give back the DownloadFileResponse but got " + msgWrapper);
        }
        Messages.DownloadFileResponse downloadFileResponseMsg = msgWrapper.getDownloadFileResponseMsg();

        SortedSet<Chunk> chunks = downloadChunks(filename, downloadFileResponseMsg);

        logger.info("Assembling chunks into file " + filename);
        File file = Chunk.createFileFromChunks(chunks, filename);
        long bytes = Files.size(file.toPath());
        double megabytes = bytes / 1e6;
        megabytes = ((int) Math.round(100 * megabytes)) / 100.0; // round to two decimals
        logger.info("File assembled. Size: " + megabytes + " MB");

        // Cleanup
        logger.debug("Deleting all chunks from local filesystem");
        for (Chunk chunk : chunks) {
            chunk.getChunkLocalPath().toFile().delete();
        }
    }

    private static SortedSet<Chunk> downloadChunks(String filename, Messages.DownloadFileResponse downloadFileResponseMsg) throws IOException, ExecutionException, InterruptedException {

        int nThreads = DFSProperties.getInstance().getClientParallelDownloads();
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);

        // Each Thread gets its own socket
        // Key is thread ID + storage node
        Map<ThreadStorageNodeKey, Socket> sockets = new HashMap<>();

        List<Future<Chunk>> futures = new ArrayList<>();
        Map<Integer, List<ComponentAddress>> chunkLocations = parseChunkLocations(downloadFileResponseMsg);
        for (Map.Entry<Integer, List<ComponentAddress>> entry : chunkLocations.entrySet()) {
            int sequenceNo = entry.getKey();
            List<ComponentAddress> nodes = entry.getValue();
            ComponentAddress randomNode = Utils.chooseNrandomOrMin(1, new HashSet<>(nodes)).iterator().next();

            // Download chunk from that random node
            DownloadChunkTask task = new DownloadChunkTask(filename, sequenceNo, randomNode, sockets);
            futures.add(executor.submit(task));
        }

        SortedSet<Chunk> chunks = new TreeSet<>();

        try {
            for (int sequenceNo : chunkLocations.keySet()) {
                chunks.add(futures.get(sequenceNo).get());
            }

            for (Socket socket : sockets.values()) {
                socket.close();
            }
        } finally {

            try {
                logger.trace("Attempting to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } finally {
                if (!executor.isTerminated()) {
                    logger.error("Some tasks didn't finish.");
                }
                executor.shutdownNow();
                logger.trace("ExecutorService shutdown finished.");
            }
        }

        return chunks;
    }

    private static Chunk downloadChunk(String filename, int sequenceNo, Socket socket) throws IOException {
        Messages.MessageWrapper requestMsg = Messages.MessageWrapper.newBuilder()
                .setDownloadChunkMsg(
                        Messages.DownloadChunk.newBuilder()
                                .setFilename(filename)
                                .setSequenceNo(sequenceNo)
                                .build()
                )
                .build();
        requestMsg.writeDelimitedTo(socket.getOutputStream());

        Messages.MessageWrapper msgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
        if (!msgWrapper.hasStoreChunkMsg()) {
            throw new IllegalStateException("Response to DownloadChunk should have been StoreChunk. Got: " + TextFormat.printToString(msgWrapper));
        }

        return processStoreChunkMsg(msgWrapper);
    }

    private static class ThreadStorageNodeKey {
        private final long threadId;
        private final ComponentAddress storageNode;

        public ThreadStorageNodeKey(long threadId, ComponentAddress storageNode) {
            this.threadId = threadId;
            this.storageNode = storageNode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThreadStorageNodeKey that = (ThreadStorageNodeKey) o;
            return threadId == that.threadId &&
                    Objects.equals(storageNode, that.storageNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(threadId, storageNode);
        }
    }

    private static Chunk processStoreChunkMsg(Messages.MessageWrapper msgWrapper) throws IOException {
        Messages.StoreChunk storeChunkMsg
                = msgWrapper.getStoreChunkMsg();
        logger.debug("Storing file name: "
                + storeChunkMsg.getFileName() + " Chunk #" + storeChunkMsg.getSequenceNo());

        String storageDirectory = DFSProperties.getInstance().getClientChunksDir();
        File storageDirectoryFile = new File(storageDirectory);
        if (!storageDirectoryFile.exists()) {
            if (!storageDirectoryFile.mkdir()) {
                System.err.println("Could not create storage directory.");
                System.exit(1);
            }
        }

        // Store chunk file
        String chunkFilename = storeChunkMsg.getFileName() + "-chunk" + storeChunkMsg.getSequenceNo();
        Path chunkFilePath = Paths.get(storageDirectory, chunkFilename);
        File chunkFile = chunkFilePath.toFile();
        if (chunkFile.exists()) {
            if (!chunkFile.delete()) {
                throw new RuntimeException("Unable to delete existing file before overwriting");
            }
        }
        logger.debug("Storing to file " + chunkFilePath);
        FileOutputStream fos = new FileOutputStream(chunkFile);
        storeChunkMsg.getData().writeTo(fos);
        fos.close();

        Utils.checkSum(chunkFile, storeChunkMsg.getChecksum());

        return new Chunk(storeChunkMsg.getFileName(), storeChunkMsg.getSequenceNo(), Files.size(chunkFilePath), storeChunkMsg.getChecksum(), chunkFilePath);
    }

    private static void sendChunkedSampleFile(String filename, GetStorageNodeListRunnable storageNodeListRunnable) throws IOException, InterruptedException {

        List<ComponentAddress> storageNodeAddresses = fetchStorageNodes(storageNodeListRunnable);

        int storageNodeIndex = random.nextInt(storageNodeAddresses.size());
        int nbStorageNodes = storageNodeAddresses.size();

        Chunk[] chunks = Chunk.createChunksFromFile(
                filename,
                DFSProperties.getInstance().getChunkSize(),
                DFSProperties.getInstance().getClientChunksDir());
        for (Chunk chunk : chunks) {
            int i = (storageNodeIndex + 1) % nbStorageNodes;
            storageNodeIndex = i;
            logger.trace("Will send chunk " + chunk.getSequenceNo() + " to node #" + i);

            ComponentAddress storageNodeAddr = storageNodeAddresses.get(i);

            logger.debug("Connecting to storage node " + storageNodeAddr);
            Socket socket = storageNodeAddr.getSocket();

            logger.debug("Sending file '" + chunk.getFilename() + "' to storage node " + storageNodeAddr);
            // Read chunk data from disk
            File chunkFile = chunk.getChunkLocalPath().toFile();
            FileInputStream fis = new FileInputStream(chunkFile);
            ByteString data = ByteString.readFrom(fis);
            fis.close();

            Messages.StoreChunk storeChunkMsg
                    = Messages.StoreChunk.newBuilder()
                    .setFileName(chunk.getFilename())
                    .setSequenceNo(chunk.getSequenceNo())
                    .setChecksum(chunk.getChecksum())
                    .setData(data)
                    .build();

            Messages.MessageWrapper msgWrapper =
                    Messages.MessageWrapper.newBuilder()
                            .setStoreChunkMsg(storeChunkMsg)
                            .build();

            msgWrapper.writeDelimitedTo(socket.getOutputStream());

            logger.debug("Close connection to storage node " + storageNodeAddr.getHost());
            logger.debug("Deleting chunk file " + chunkFile.getName());
            if (!chunkFile.delete()) {
                logger.warn("Chunk file " + chunkFile.getName() + " could not be deleted.");
            }
            socket.close();
        }
    }

    private static List<ComponentAddress> fetchStorageNodes(GetStorageNodeListRunnable storageNodeListRunnable) throws InterruptedException {
        List<ComponentAddress> storageNodeAddresses;

        try {
            while ((storageNodeAddresses = storageNodeListRunnable.getStorageNodeAddresses()) == null) {
                storageNodeAddressesAvailableSema.acquire();
            }
        } finally {
            storageNodeAddressesAvailableSema.release();
        }
        return storageNodeAddresses;
    }

    private static Map<Integer, List<ComponentAddress>> parseChunkLocations(Messages.DownloadFileResponse downloadFileResponseMsg) {
        Map<Integer, List<ComponentAddress>> result = new HashMap<>();
        for (Messages.DownloadFileResponse.ChunkLocation chunkLocation : downloadFileResponseMsg.getChunkLocationsList()) {
            List<ComponentAddress> nodes = new ArrayList<>();
            for (Messages.StorageNode node : chunkLocation.getStorageNodesList()) {
                nodes.add(new ComponentAddress(node.getHost(), node.getPort()));
            }
            logger.debug("Chunk " + chunkLocation.getSequenceNo() + " is on " + nodes);
            result.put(chunkLocation.getSequenceNo(), nodes);
        }
        return result;
    }

    private static void sendFile(ComponentAddress controllerAddr, String filename) throws IOException, InterruptedException {
        GetStorageNodeListRunnable storageNodeListRunnable = new GetStorageNodeListRunnable(controllerAddr, storageNodeAddressesAvailableSema);
        new Thread(storageNodeListRunnable).start();
        sendChunkedSampleFile(filename, storageNodeListRunnable);
    }

    private static void printHelp() throws IOException {
        StringBuilder sb = new StringBuilder();
        InputStream is = Client.class.getClassLoader().getResourceAsStream("help.txt");
        char[] buf = new char[1024];
        int c;

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        while ((c = reader.read(buf)) != -1) {
            sb.append(new String(buf, 0, c));
        }
        reader.close();

        System.err.println(sb.toString());
    }

    private static class DownloadChunkTask implements Callable<Chunk> {

        private final String filename;
        private final int sequenceNo;
        private final ComponentAddress storageNode;
        private final Map<ThreadStorageNodeKey, Socket> sockets;

        public DownloadChunkTask(String filename, int sequenceNo, ComponentAddress storageNode, Map<ThreadStorageNodeKey, Socket> sockets) throws IOException {
            this.filename = filename;
            this.sequenceNo = sequenceNo;
            this.storageNode = storageNode;
            this.sockets = sockets;
        }

        @Override
        public Chunk call() throws Exception {
            /*
            long threadId = Thread.currentThread().getId();
            ThreadStorageNodeKey key = new ThreadStorageNodeKey(threadId, storageNode);
            if (sockets.get(key) == null) {
                sockets.put(key, storageNode.getSocket());
            }
            Socket socket = sockets.get(key);
            */
            Socket socket = storageNode.getSocket();

            return downloadChunk(filename, sequenceNo, socket);
        }
    }

    static List<ComponentAddress> toComponentAddresses(List<Messages.StorageNode> list) {
        List<ComponentAddress> addresses = new ArrayList<>(list.size());
        for (Messages.StorageNode storageNode : list) {
            addresses.add(toComponentAddress(storageNode));
        }
        return addresses;
    }

    private static ComponentAddress toComponentAddress(Messages.StorageNode node) {
        return new ComponentAddress(node.getHost(), node.getPort());
    }

}
