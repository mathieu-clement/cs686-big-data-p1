package edu.usfca.cs.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class DFSProperties {
    private static final DFSProperties INSTANCE = new DFSProperties();
    private static final Logger logger = LoggerFactory.getLogger(DFSProperties.class);
    private final Properties properties;

    private DFSProperties() {
        properties = new Properties();
        try {
            properties.load(DFSProperties.class.getClassLoader()
                    .getResourceAsStream("dfs.properties"));
        } catch (IOException e) {
            logger.error("Couldn't load properties from file", e);
        }
    }

    public static DFSProperties getInstance() {
        return INSTANCE;
    }

    public int getHeartbeatPeriod() {
        return parseInt(getProperty("heartbeat-period"));
    }

    public int getReplicationCheckPeriod() {
        return parseInt(getProperty("replication-check-period"));
    }

    public long getChunkSize() {
        return Long.parseLong(getProperty("chunk-size"));
    }

    public String getClientChunksDir() {
        return getProperty("client-chunks-dir");
    }

    public String getStorageNodeChunksDir() {
        return getProperty("storage-node-chunks-dir");
    }

    public int getMinReplicas() {
        return parseInt(getProperty("min-replicas"));
    }

    public boolean isOverwriteOutputFile() {
        return "true".equals(getProperty("overwrite-output-file"));
    }

    public int getClientParallelDownloads() {
        return parseInt(getProperty("client-parallel-downloads"));
    }

    public int getHeartbeatCheckPeriod() {
        return parseInt(getProperty("heartbeat-check-period"));
    }

    public int getMaxHeartbeatAge() {
        return parseInt(getProperty("max-heartbeat-age"));
    }

    private String getProperty(String name) {
        return System.getProperty(name, properties.getProperty(name));
    }
}
