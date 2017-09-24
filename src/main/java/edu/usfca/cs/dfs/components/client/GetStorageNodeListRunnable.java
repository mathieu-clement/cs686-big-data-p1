package edu.usfca.cs.dfs.components.client;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

class GetStorageNodeListRunnable implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(GetStorageNodeListRunnable.class);
    private final ComponentAddress controllerAddr;

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

                List<ComponentAddress> storageNodes = Client.toComponentAddresses(responseMsg.getNodesList());
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
