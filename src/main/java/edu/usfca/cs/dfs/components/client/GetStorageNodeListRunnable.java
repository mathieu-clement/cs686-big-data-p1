package edu.usfca.cs.dfs.components.client;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Semaphore;

class GetStorageNodeListRunnable implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(GetStorageNodeListRunnable.class);
    private final ComponentAddress controllerAddr;
    private final Semaphore storageNodeAddressesAvailableSema;
    private List<ComponentAddress> storageNodeAddresses;

    public GetStorageNodeListRunnable(ComponentAddress controllerAddr, Semaphore storageNodeAddressesAvailableSema) {
        this.controllerAddr = controllerAddr;
        this.storageNodeAddressesAvailableSema = storageNodeAddressesAvailableSema;
    }

    public static List<ComponentAddress> fetchStorageNodes(ComponentAddress controllerAddr) throws IOException {
        Socket socket = controllerAddr.getSocket();
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

        return Client.toComponentAddresses(responseMsg.getNodesList());
    }

    @Override
    public void run() {
        try {
            storageNodeAddresses = fetchStorageNodes(controllerAddr);
            storageNodeAddressesAvailableSema.release();

            logger.debug("Received list of storage nodes: " + storageNodeAddresses);
        } catch (IOException ioe) {
            logger.error("Could not get storage node list from controller", ioe);
        }
    }

    public List<ComponentAddress> getStorageNodeAddresses() {
        return storageNodeAddresses;
    }
}
