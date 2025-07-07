package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ReplicationLogReplayService {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogReplayService.class);

    /** The path on the HDFS where log files are to be read. */
    public static final String REPLICATION_LOG_REPLAY_HDFS_URL_KEY =
            "phoenix.replication.log.replay.hdfs.url";

    private static volatile ReplicationLogReplayService instance;
    private final Configuration conf;

    private ReplicationLogReplayService(final Configuration conf) {
        // TODO Check if replication replay service is enabled via config
        this.conf = conf;
    }

    /**
     * Gets the singleton instance of the ReplicationLogReplayService using the lazy initializer pattern.
     * Initializes the instance if it hasn't been created yet.
     * @param conf Configuration object.
     * @return The singleton ReplicationLogManager instance.
     * @throws IOException If initialization fails.
     */
    public static ReplicationLogReplayService getInstance(Configuration conf)
            throws IOException {
        if (instance == null) {
            synchronized (ReplicationLogReplayService.class) {
                if (instance == null) {
                    // Complete initialization before assignment
                    ReplicationLogReplayService replicationLogReplayService = new ReplicationLogReplayService(conf);
                    replicationLogReplayService.initializeFileSystem();
                    instance = replicationLogReplayService;
                }
            }
        }
        return instance;
    }

    public void init() throws IOException {
        // Initialize the file system and root directory
        initializeFileSystem();
    }

    /** Initializes the filesystem and creates root log directory. */
    protected void initializeFileSystem() throws IOException {
        String uriString = conf.get(REPLICATION_LOG_REPLAY_HDFS_URL_KEY);
        if (uriString == null) {
            throw new IOException(REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not configured");
        }
        try {
            URI uri = new URI(uriString);
            FileSystem fileSystem = FileSystem.get(uri, conf);
            Path logDirectoryPath = new Path(uri.getPath());
            if (!fileSystem.exists(logDirectoryPath)) {
                LOG.info("Creating directory {}", logDirectoryPath);
                if (!fileSystem.mkdirs(logDirectoryPath)) {
                    throw new IOException("Failed to create directory: " + uriString);
                }
            }
        } catch (URISyntaxException e) {
            throw new IOException(REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not valid", e);
        }
    }

    public void start() throws IOException {
        // TODO: Ensure service is not already started
        List<String> replicationGroups = getReplicationGroups();
        for(String replicationGroup : replicationGroups) {
            ReplicationLogReplay replay = new ReplicationLogReplay(conf, replicationGroup);
            replay.start();
        }
    }

    public void stop() {
        // Stop log replay for all groups
    }

    protected List<String> getReplicationGroups() {
        // TODO: Return list of replication groups using HAGroupStoreClient
        return new ArrayList<>();
    }
}
