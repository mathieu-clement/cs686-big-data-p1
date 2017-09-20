package edu.usfca.cs.dfs.components;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.StorageNodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args)
    throws Exception {
        /*if (args.length != 2) {
            System.err.println("Two arguments required: controller-address controller-listening-port");
            System.exit(1);
        }

        String controllerAddr = args[0];
        int controllerPort = Integer.parseInt(args[1]);
        */

        if (args.length < 2) {
            System.err.println("Usage: Client fileToSend storageNode1:port [storageNode2:port]...");
            System.exit(1);
        }

        String filename = args[0];
        StorageNodeAddress[] storageNodeAddresses = parseStorageNodeAddressesFromArgs(args);

        sendChunkedSampleFile(filename, storageNodeAddresses);
    }

    private static StorageNodeAddress[] parseStorageNodeAddressesFromArgs(String[] args) {
        int nbStorageNodes = args.length - 1;
        StorageNodeAddress[] storageNodeAddresses = new StorageNodeAddress[nbStorageNodes];
        for (int i = 1; i < args.length; i++) {
            String[] split = args[i].split(":");
            storageNodeAddresses[i - 1] = new StorageNodeAddress(split[0], Integer.parseInt(split[1]));
        }
        return storageNodeAddresses;
    }

    private static void sendChunkedSampleFile(String filename, StorageNodeAddress... storageNodeAddresses) throws IOException {

        int storageNodeIndex = 0;
        int nbStorageNodes = storageNodeAddresses.length;

        Chunk[] chunks = Chunk.createChunksFromFile(filename, (long) 1e6, "/tmp/output");
        for (Chunk chunk : chunks) {
            int i = (storageNodeIndex + 1) % nbStorageNodes;
            storageNodeIndex = i;
            logger.trace("Will send chunk " + chunk.getSequenceNo() + " to node #" + i);

            StorageNodeAddress storageNodeAddr = storageNodeAddresses[i];
            String storageNodeHost = storageNodeAddr.getHost();
            int storageNodePort = storageNodeAddr.getPort();

            logger.debug("Connecting to storage node " + storageNodeAddr);
            Socket sock = new Socket(storageNodeHost, storageNodePort);

            logger.debug("Sending file 'my_file.txt' with data 'Hello World' to storage node " + storageNodeAddr);
            // Read chunk data from disk
            FileInputStream fis = new FileInputStream(chunk.getChunkLocalPath().toFile());
            ByteString data = ByteString.readFrom(fis);
            fis.close();

            Messages.StoreChunk storeChunkMsg
                    = Messages.StoreChunk.newBuilder()
                    .setFileName(chunk.getFilename())
                    .setSequenceNo(chunk.getSequenceNo())
                    .setData(data)
                    .build();

            Messages.MessageWrapper msgWrapper =
                    Messages.MessageWrapper.newBuilder()
                            .setStoreChunkMsg(storeChunkMsg)
                            .build();

            msgWrapper.writeDelimitedTo(sock.getOutputStream());

            logger.debug("Close connection to storage node " + storageNodeHost);
            sock.close();
        }
    }
}
