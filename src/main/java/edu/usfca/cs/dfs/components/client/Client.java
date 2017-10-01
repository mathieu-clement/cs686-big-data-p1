package edu.usfca.cs.dfs.components.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class Client {

    private static final Random random = new Random();

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static Semaphore storageNodeAddressesAvailableSema = new Semaphore(0);

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: Client controller-host controller-port fileToSend");
            System.exit(1);
        }

        ComponentAddress controllerAddr = new ComponentAddress(args[0], Integer.parseInt(args[1]));
        String filename = args[2];
        GetStorageNodeListRunnable storageNodeListRunnable = new GetStorageNodeListRunnable(controllerAddr, storageNodeAddressesAvailableSema);
        new Thread(storageNodeListRunnable).start();

        sendChunkedSampleFile(filename, storageNodeListRunnable);
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
