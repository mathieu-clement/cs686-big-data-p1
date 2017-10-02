package edu.usfca.cs.dfs.components.client;

import com.google.protobuf.ByteString;
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
import java.util.concurrent.Semaphore;

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

        String command = args[2];

        switch (command) {
            case "upload-file":
                sendFile(controllerAddr, args[3]);
                break;

            case "download-file":
                downloadFile(controllerAddr, args[3]);
                break;

            default:
                printHelp();
                System.exit(1);
        }
    }

    private static Map<ComponentAddress, Socket> storageNodeSockets = new HashMap<>();

    private static void downloadFile(ComponentAddress controllerAddr, String filename) throws IOException {
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
            throw new IllegalStateException("Controller is supposed to give back the DownloadFileResponse");
        }
        Messages.DownloadFileResponse downloadFileResponseMsg = msgWrapper.getDownloadFileResponseMsg();

        SortedSet<Chunk> chunks = new TreeSet<>();

        Map<Integer, List<ComponentAddress>> chunkLocations = parseChunkLocations(downloadFileResponseMsg);
        for (Map.Entry<Integer, List<ComponentAddress>> entry : chunkLocations.entrySet()) {
            int sequenceNo = entry.getKey();
            List<ComponentAddress> nodes = entry.getValue();
            ComponentAddress randomNode = Utils.chooseNrandomOrMin(1, new HashSet<>(nodes)).iterator().next();
            // Download chunk from that random node
            Chunk chunk = downloadChunk(filename, sequenceNo, randomNode);
            chunks.add(chunk);
        }

        logger.info("Assembling chunks to file " + filename);
        Chunk.createFileFromChunks(chunks, filename);

        // Cleanup
        logger.debug("Deleting all chunks from local filesystem");
        for (Chunk chunk : chunks) {
            chunk.getChunkLocalPath().toFile().delete();
        }

        for (Socket socket : storageNodeSockets.values()) {
            socket.close();
        }
    }

    private static Chunk downloadChunk(String filename, int sequenceNo, ComponentAddress storageNode) throws IOException {
        Messages.MessageWrapper requestMsg = Messages.MessageWrapper.newBuilder()
                .setDownloadChunkMsg(
                        Messages.DownloadChunk.newBuilder()
                                .setFilename(filename)
                                .setSequenceNo(sequenceNo)
                                .build()
                )
                .build();
        Socket socket = getSocket(storageNode);
        requestMsg.writeDelimitedTo(socket.getOutputStream());

        Messages.MessageWrapper msgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
        if (!msgWrapper.hasStoreChunkMsg()) {
            throw new IllegalStateException("Response to DownloadChunk should have been StoreChunk");
        }

        return processStoreChunkMsg(socket, msgWrapper);
    }

    private static Chunk processStoreChunkMsg(Socket socket, Messages.MessageWrapper msgWrapper) throws IOException {
        Messages.StoreChunk storeChunkMsg
                = msgWrapper.getStoreChunkMsg();
        logger.debug("Storing file name: "
                + storeChunkMsg.getFileName() + " Chunk #" + storeChunkMsg.getSequenceNo() + " received from " +
                socket.getRemoteSocketAddress().toString());

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

    private static Socket getSocket(ComponentAddress storageNode) throws IOException {
        if (storageNodeSockets.get(storageNode) == null || storageNodeSockets.get(storageNode).isClosed()) {
            storageNodeSockets.put(storageNode, storageNode.getSocket());
        }
        return storageNodeSockets.get(storageNode);
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

    private static void sendChunkedSampleFile(String filename, GetStorageNodeListRunnable storageNodeListRunnable) throws IOException, InterruptedException {

        List<ComponentAddress> storageNodeAddresses;

        try {
            while ((storageNodeAddresses = storageNodeListRunnable.getStorageNodeAddresses()) == null) {
                storageNodeAddressesAvailableSema.acquire();
            }
        } finally {
            storageNodeAddressesAvailableSema.release();
        }

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
            Socket sock = storageNodeAddr.getSocket();

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

            msgWrapper.writeDelimitedTo(sock.getOutputStream());

            logger.debug("Close connection to storage node " + storageNodeAddr.getHost());
            logger.debug("Deleting chunk file " + chunkFile.getName());
            if (!chunkFile.delete()) {
                logger.warn("Chunk file " + chunkFile.getName() + " could not be deleted.");
            }
            sock.close();
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
