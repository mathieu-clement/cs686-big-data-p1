package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class StorageNode {

    private final static Logger logger = LoggerFactory.getLogger(StorageNode.class);

    private ServerSocket srvSocket;

    private final int port;
    private final ComponentAddress controllerAddr;

    // Map of chunks. Key is filename.
    private final Map<String, SortedSet<Chunk>> chunks = new HashMap<>();

    public StorageNode(int port, ComponentAddress controllerAddr) {
        this.port = port;
        this.controllerAddr = controllerAddr;
    }

    public static void main(String[] args)
            throws Exception {
        if (args.length != 3) {
            System.err.println("This program requires 3 arguments: storage-node-listening-port controller-address controller-listening-port");
            System.exit(1);
        }
        String hostname = getHostname();
        int port = Integer.parseInt(args[0]);
        String controllerHost = args[1];
        int controllerPort = Integer.parseInt(args[2]);
        ComponentAddress controllerAddr = new ComponentAddress(controllerHost, controllerPort);

        logger.info("Starting storage node on " + hostname + " port " + port + "...");
        logger.info("Will connect to controller " + controllerAddr);
        new StorageNode(port, controllerAddr).start();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
            throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    public void start()
            throws Exception {
        new Thread(new HeartbeatRunnable(new ComponentAddress(getHostname(), port), controllerAddr, chunks)).start();

        srvSocket = new ServerSocket(port);
        logger.debug("Listening on port " + port + "...");
        while (true) {
            Socket socket = srvSocket.accept();
            new Thread(new ProcessMessageRunnable(socket, chunks)).start();
        }
    }

}
