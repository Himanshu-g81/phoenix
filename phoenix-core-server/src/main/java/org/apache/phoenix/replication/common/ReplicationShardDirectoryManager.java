package org.apache.phoenix.replication.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.reader.Round;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationShardDirectoryManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationShardDirectoryManager.class);

    /**
     * The number of shards (subfolders) to maintain in the "IN" / "OUT" directory.
     * <p>
     * Shard directories have the format N, e.g. 0, 1, 2, etc.
     */
    public static final String REPLICATION_NUM_SHARDS_KEY = "phoenix.replication.log.shards";

    public static final int DEFAULT_REPLICATION_NUM_SHARDS = 128;

    public static final String SHARD_DIR_FORMAT = "%03d";

    public static final String REPLICATION_ROUND_TIME_PERIOD_SECONDS_KEY = "phoenix.replication.round.time.period.seconds";

    public static final int DEFAULT_REPLICATION_ROUND_TIME_PERIOD_SECONDS = 60;

    private final Path rootPath;

    private final int numShards;

    private final int roundTimeSeconds;

    public ReplicationShardDirectoryManager(final Configuration conf, final Path rootPath) {
        this.rootPath = rootPath;
        this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
        this.roundTimeSeconds = conf.getInt(REPLICATION_ROUND_TIME_PERIOD_SECONDS_KEY, DEFAULT_REPLICATION_ROUND_TIME_PERIOD_SECONDS);
    }

    public Path getShardDirectory(long fileTimestamp) {
        return rootPath;
    }

    public long getNearestRoundStartTimestamp(long timestamp) {
        // Convert round time from seconds to milliseconds
        long roundTimeMs = roundTimeSeconds * 1000L;
        
        // Calculate the nearest round start timestamp
        // This rounds down to the nearest multiple of round time
        return (timestamp / roundTimeMs) * roundTimeMs;
    }

//    private String getShardSubDirectoryName(long timestamp) {
//        return String.format(SHARD_DIR_FORMAT, getNearestRoundStartTimestamp(timestamp));
//    }

    public long getRoundTimeSeconds() {
        return roundTimeSeconds;
    }

//    public Path getInRoundDirectory(Round round) {
//        long startTime = round.getStartTime();
//        // Convert timestamp to minutes since start of day (00:00:00)
//        // Assuming timestamp is in milliseconds since epoch
//        long minutesSinceEpoch = startTime / (60 * 1000); // Convert to minutes
//        long minutesSinceStartOfDay = minutesSinceEpoch % (24 * 60); // Get minutes within the day (0-1439)
//
//        // Calculate which shard directory this timestamp belongs to
//        // Each shard represents a time range: 0-1 min = shard 0, 1-2 min = shard 1, etc.
//        int shardIndex = (int) (minutesSinceStartOfDay % numShards);
//
//        // Create the path: standbyUrl/shardIndex
//        return new Path(standbyUrl.getPath(), String.valueOf(shardIndex));
//    }

//    public long getRoundStartTime() {
//
//    }
//
//    public FileSystem getStandbyFs() {
//        return standbyFs;
//    }

//    public Round getRoundForStartTime(long startTime) {
//        long nearestStartTime = getNearestRoundStartTimestamp(startTime);
//        return new Round(nearestStartTime,  roundTimeSeconds);
//    }
}
