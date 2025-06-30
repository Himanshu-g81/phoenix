package org.apache.phoenix.replication.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.phoenix.replication.reader.Round;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ReplicationContext {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationContext.class);

    /** The path on the standby HDFS where log files should be written. (The "IN" directory.) */
    public static final String REPLICATION_STANDBY_HDFS_URL_KEY =
            "phoenix.replication.log.standby.hdfs.url";

    /**
     * The number of shards (subfolders) to maintain in the "IN" directory.
     * <p>
     * Shard directories have the format N, e.g. 0, 1, 2, etc. The maximum value is
     * 512.
     */
    public static final String REPLICATION_NUM_SHARDS_KEY = "phoenix.replication.log.shards";

    public static final int DEFAULT_REPLICATION_NUM_SHARDS = 128;

    public static final int MAX_REPLICATION_NUM_SHARDS = 512;

    protected final Configuration conf;
    protected FileSystem standbyFs;
    protected int numShards;
    protected URI standbyUrl;

    public ReplicationContext(Configuration conf) {
        this.conf = conf;
        this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
        if (numShards > MAX_REPLICATION_NUM_SHARDS) {
            throw new IllegalArgumentException(REPLICATION_NUM_SHARDS_KEY + " is " + numShards
                    + ", but the limit is " + MAX_REPLICATION_NUM_SHARDS);
        }
    }

    public void init() throws IOException {
        initializeFileSystems();
    }

    /** Initializes the standby and fallback filesystems and creates their log directories. */
    protected void initializeFileSystems() throws IOException {
        String standbyUrlString = conf.get(REPLICATION_STANDBY_HDFS_URL_KEY);
        if (standbyUrlString == null) {
            throw new IOException(REPLICATION_STANDBY_HDFS_URL_KEY + " is not configured");
        }
        // Only validate that the URI is well-formed. We should not assume the scheme must be
        // "hdfs" because perhaps the operator will substitute another FileSystem implementation
        // for DistributedFileSystem.
        try {
            this.standbyUrl = new URI(standbyUrlString);
        } catch (URISyntaxException e) {
            throw new IOException(REPLICATION_STANDBY_HDFS_URL_KEY + " is not valid", e);
        }
        // Configuration is sorted, and possibly store-and-forward directories have been created,
        // now create the standby side directories as needed.
        this.standbyFs = getFileSystem(standbyUrl);
        Path standbyLogDir = new Path(standbyUrl.getPath());
        if (!standbyFs.exists(standbyLogDir)) {
            LOG.info("Creating directory {}", standbyUrlString);
            if (!standbyFs.mkdirs(standbyLogDir)) {
                throw new IOException("Failed to create directory: " + standbyUrlString);
            }
        }
    }

    /** Gets a FileSystem instance for the given URI using the current configuration. */
    protected FileSystem getFileSystem(URI uri) throws IOException {
        return FileSystem.get(uri, conf);
    }

    public Path getInRoundDirectory(Round round) {
        long startTime = round.getStartTime();
        // Convert timestamp to minutes since start of day (00:00:00)
        // Assuming timestamp is in milliseconds since epoch
        long minutesSinceEpoch = startTime / (60 * 1000); // Convert to minutes
        long minutesSinceStartOfDay = minutesSinceEpoch % (24 * 60); // Get minutes within the day (0-1439)
        
        // Calculate which shard directory this timestamp belongs to
        // Each shard represents a time range: 0-1 min = shard 0, 1-2 min = shard 1, etc.
        int shardIndex = (int) (minutesSinceStartOfDay % numShards);
        
        // Create the path: standbyUrl/shardIndex
        return new Path(standbyUrl.getPath(), String.valueOf(shardIndex));
    }

    public FileSystem getStandbyFs() {
        return standbyFs;
    }
}
