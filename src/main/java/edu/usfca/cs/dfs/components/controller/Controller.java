package edu.usfca.cs.dfs.components.controller;

import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    private final int port;

    private final Set<ComponentAddress> onlineStorageNodes = new HashSet<>();

    private final FileTable fileTable = new FileTable();

    public Controller(int port) {
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: Controller port");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);

        logger.info("Starting controller...");

        new Controller(port).start();
    }

    public void start() throws Exception {
        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new ProcessIncomingMessageRunnable(onlineStorageNodes, socket)).start();
        }
    }

}
