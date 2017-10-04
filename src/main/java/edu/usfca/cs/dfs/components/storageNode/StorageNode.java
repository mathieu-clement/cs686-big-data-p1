package edu.usfca.cs.dfs.components.storageNode;

import edu.usfca.cs.dfs.DFSProperties;
import edu.usfca.cs.dfs.Utils;
import edu.usfca.cs.dfs.structures.Chunk;
import edu.usfca.cs.dfs.structures.ComponentAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StorageNode {

    private final static Logger logger = LoggerFactory.getLogger(StorageNode.class);

    private ServerSocket srvSocket;

    private final int port;
    private final ComponentAddress controllerAddr;

    // Map of chunks. Key is filename.
    private final Map<String, SortedSet<Chunk>> chunks;

    public StorageNode(int port, ComponentAddress controllerAddr) throws IOException {
        this.port = port;
        this.controllerAddr = controllerAddr;
        this.chunks = readChunks();
    }

    public static void addToChunks(Chunk chunk, Map<String, SortedSet<Chunk>> chunks, Lock lock) {
        String filename = chunk.getFilename();
        lock.lock();
        try {
            if (chunks.get(filename) == null) {
                chunks.put(filename, new TreeSet<Chunk>());
            }
        } finally {
            lock.unlock();
        }
        chunks.get(filename).add(chunk);
    }

    private Map<String, SortedSet<Chunk>> readChunks() throws IOException {
        Map<String, SortedSet<Chunk>> result = new HashMap<>();
        Lock lock = new ReentrantLock();
        Path chunksPath = Paths.get(DFSProperties.getInstance().getStorageNodeChunksDir());
        File chunksDirFile = chunksPath.toFile();
        Pattern chunkFileNamePattern = Pattern.compile("(.*?)-chunk([0-9]+)");

        if (!chunksDirFile.exists()) return result;
        if (!chunksDirFile.isDirectory()) throw new IllegalArgumentException("Chunks directory is a regular file");
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(chunksPath);
        for (Path path : directoryStream) {
            // Ignore md5 files
            if (path.toString().endsWith(".md5")) continue;

            // Extract info from name
            File chunkFile = path.toFile();
            Matcher matcher = chunkFileNamePattern.matcher(chunkFile.getName());
            if (!matcher.find()) {
                throw new IllegalArgumentException("Malformed chunk file name " + chunkFile.getName());
            }
            String originalFileName = matcher.group(1);
            int sequenceNo = Integer.parseInt(matcher.group(2));

            // Create chunk
            Chunk chunk = new Chunk(originalFileName, sequenceNo, chunkFile.length(), path);
            chunk.calculateAndSetChecksum();

            // Check sum
            Path checksumFilePath = Paths.get(path.toString() + ".md5");
            String expectedChecksum = new String(Files.readAllBytes(checksumFilePath)).split(" ")[0];
            Utils.checkSum(chunkFile, expectedChecksum);

            addToChunks(chunk, result, lock);
        }

        logger.debug("Chunks at startup: " + result);

        return result;
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
        return InetAddress.getLocalHost().getHostName()
                ;
    }

    public void start()
            throws Exception {
        Lock chunksLock = new ReentrantLock();
        new Thread(new HeartbeatRunnable(new ComponentAddress(getHostname(), port), controllerAddr, chunks)).start();

        srvSocket = new ServerSocket(port);
        logger.debug("Listening on port " + port + "...");
        while (true) {
            Socket socket = srvSocket.accept();
            logger.trace("New connection from " + socket.getRemoteSocketAddress());
            new Thread(new MessageProcessor(socket, chunks, chunksLock)).start();
        }
    }

}
