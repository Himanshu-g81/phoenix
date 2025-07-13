package org.apache.phoenix.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Manage any shard based directory (IN or OUT)
 * rootURI = /phoenix/replication/
 * groupName = "default"
 * numShards = 128
 * roundTimeSeconds = 60
 * shardDirectory = /phoenix/replication/<group-id>/shard/000
 * shardDirectory = /phoenix/replication/<group-id>/shard/001

 */
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

    private static final String REPLICATION_SHARD_SUB_DIRECTORY_NAME = "shard";

    private final int numShards;

    private final int roundTimeSeconds;

    private final Path shardDirectoryPath;

    public ReplicationShardDirectoryManager(final Configuration conf, final Path rootPath) {
        this.shardDirectoryPath = new Path(rootPath.toUri().getPath(), REPLICATION_SHARD_SUB_DIRECTORY_NAME);
        this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
        this.roundTimeSeconds = conf.getInt(REPLICATION_ROUND_TIME_PERIOD_SECONDS_KEY, DEFAULT_REPLICATION_ROUND_TIME_PERIOD_SECONDS);
    }

    /**
     * Returns the shard directory to which file with given timestamp belongs to based on round time period
     * @param fileTimestamp The timestamp in milliseconds since epoch
     * @return The shard directory path for the given timestamp
     */
    public Path getShardDirectory(long fileTimestamp) {
        // 1. Figure out how many seconds have passed from start of the day for this file
        // Convert timestamp to seconds since epoch
        long secondsSinceEpoch = fileTimestamp / 1000L;
        
        // Calculate seconds since start of the day (00:00:00)
        // Get the number of seconds since the start of the current day
        long secondsSinceStartOfDay = secondsSinceEpoch % TimeUnit.DAYS.toSeconds(1);
        
        // 2. Calculate which shard this timestamp belongs to
        // Each shard represents a time range: 0 to roundTimeSeconds = shard 0, 
        // roundTimeSeconds to 2*roundTimeSeconds = shard 1, etc.
        int shardIndex = (int) (secondsSinceStartOfDay / roundTimeSeconds);
        
        // Apply modulo to ensure shard index doesn't exceed numShards
        shardIndex = shardIndex % numShards;
        
        // Create the shard directory path with formatted shard number
        String shardDirName = String.format(SHARD_DIR_FORMAT, shardIndex);
        return new Path(shardDirectoryPath, shardDirName);
    }

    public Path getShardDirectory(Round round) {
        return getShardDirectory(round.getStartTime());
    }

    public long getNearestRoundStartTimestamp(long timestamp) {
        // Convert round time from seconds to milliseconds
        long roundTimeMs = roundTimeSeconds * 1000L;
        
        // Calculate the nearest round start timestamp
        // This rounds down to the nearest multiple of round time
        return (timestamp / roundTimeMs) * roundTimeMs;
    }

    public Round getReplicationRoundFromStartTime(long roundStartTime) {
        long validRoundStartTime = getNearestRoundStartTimestamp(roundStartTime);
        long validRoundEndTime = roundStartTime + roundTimeSeconds * 1000L;
        return new Round(validRoundStartTime, validRoundEndTime);
    }

    public Round getReplicationRoundFromEndTime(long roundEndTime) {
        long validRoundEndTime = getNearestRoundStartTimestamp(roundEndTime);
        long validRoundStartTime = validRoundEndTime - roundTimeSeconds * 1000L;
        return new Round(validRoundStartTime, validRoundEndTime);
    }

    public List<Path> getAllShardPaths() {
        List<Path> shardPaths = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            String shardDirName = String.format(SHARD_DIR_FORMAT, i);
            Path shardPath = new Path(shardDirectoryPath, shardDirName);
            shardPaths.add(shardPath);
        }
        return shardPaths;
    }

    public long getRoundTimeSeconds() {
        return roundTimeSeconds;
    }
}
