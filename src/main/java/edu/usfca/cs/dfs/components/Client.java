package edu.usfca.cs.dfs.components;

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
import java.util.concurrent.Semaphore;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private Semaphore storageNodeListReceived = new Semaphore(0);

    public static void main(String[] args)
            throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: Client controller-host controller-port fileToSend storageNode1:port [storageNode2:port]...");
            System.exit(1);
        }

        ComponentAddress controllerAddr = new ComponentAddress(args[0], Integer.parseInt(args[1]));
        String filename = args[2];
        ComponentAddress[] storageNodeAddresses = parseStorageNodeAddressesFromArgs(3, args);

        new Thread(new GetStorageNodeListRunnable(controllerAddr)).start();


        sendChunkedSampleFile(filename, storageNodeAddresses);
    }

    private static ComponentAddress[] parseStorageNodeAddressesFromArgs(int startIndex, String[] args) {
        int nbStorageNodes = args.length - startIndex;
        ComponentAddress[] storageNodeAddresses = new ComponentAddress[nbStorageNodes];
        for (int i = startIndex; i < args.length; i++) {
            String[] split = args[i].split(":");
            storageNodeAddresses[i - startIndex] = new ComponentAddress(split[0], Integer.parseInt(split[1]));
        }
        return storageNodeAddresses;
    }

    private static void sendChunkedSampleFile(String filename, ComponentAddress... storageNodeAddresses) throws IOException {

        int storageNodeIndex = 0;
        int nbStorageNodes = storageNodeAddresses.length;

        Chunk[] chunks = Chunk.createChunksFromFile(
                filename,
                DFSProperties.getInstance().getChunkSize(),
                DFSProperties.getInstance().getClientChunksDir());
        for (Chunk chunk : chunks) {
            int i = (storageNodeIndex + 1) % nbStorageNodes;
            storageNodeIndex = i;
            logger.trace("Will send chunk " + chunk.getSequenceNo() + " to node #" + i);

            ComponentAddress storageNodeAddr = storageNodeAddresses[i];

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
                    .setData(data)
                    .build();

            Messages.MessageWrapper msgWrapper =
                    Messages.MessageWrapper.newBuilder()
                            .setStoreChunkMsg(storeChunkMsg)
                            .build();

            msgWrapper.writeDelimitedTo(sock.getOutputStream());

            logger.debug("Close connection to storage node " + storageNodeAddr.getHost());
            logger.debug("Deleting chunk file " + chunkFile.getName());
            chunkFile.delete();
            sock.close();
        }
    }

    private static List<ComponentAddress> toComponentAddresses(List<Messages.GetStorageNodesResponse.StorageNode> list) {
        List<ComponentAddress> addresses = new ArrayList<>(list.size());
        for (Messages.GetStorageNodesResponse.StorageNode storageNode : list) {
            addresses.add(toComponentAddress(storageNode));
        }
        return addresses;
    }

    private static ComponentAddress toComponentAddress(Messages.GetStorageNodesResponse.StorageNode node) {
        return new ComponentAddress(node.getHost(), node.getPort());
    }

    private static class GetStorageNodeListRunnable implements Runnable {
        private final ComponentAddress controllerAddr;
        private final static Logger logger = LoggerFactory.getLogger(GetStorageNodeListRunnable.class);

        public GetStorageNodeListRunnable(ComponentAddress controllerAddr) {
            this.controllerAddr = controllerAddr;
        }

        @Override
        public void run() {
            try {
                Socket socket = controllerAddr.getSocket();
                while (true) {
                    Messages.GetStorageNodesRequest storageNodesRequestMsg = Messages.GetStorageNodesRequest.newBuilder().build();
                    Messages.MessageWrapper sentMsgWrapper = Messages.MessageWrapper.newBuilder()
                            .setGetStoragesNodesRequestMsg(storageNodesRequestMsg)
                            .build();

                    logger.debug("Asking for list of storage nodes...");
                    sentMsgWrapper.writeDelimitedTo(socket.getOutputStream());

                    logger.debug("Waiting for list of storage nodes...");
                    Messages.MessageWrapper receivedMsgWrapper = Messages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
                    if (!receivedMsgWrapper.hasGetStorageNodesResponseMsg()) {
                        throw new UnsupportedOperationException("Expected storage node list response, but got something else.");
                    }
                    Messages.GetStorageNodesResponse responseMsg = receivedMsgWrapper.getGetStorageNodesResponseMsg();

                    List<ComponentAddress> storageNodes = toComponentAddresses(responseMsg.getNodesList());
                    logger.debug("Received list of storage nodes: " + storageNodes);

                    Thread.sleep(10000);
                }
            } catch (IOException ioe) {
                logger.error("Could not get storage node list from controller", ioe);
            } catch (InterruptedException e) {
                logger.warn("Exception while sleeping", e);
            }
        }
    }
}
