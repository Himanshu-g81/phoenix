package org.apache.phoenix.replication;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.reader.Round;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ReplicationStateTracker {

    private Round lastSuccessfullyProcessedRound;

    public void init(ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        initLastSuccessfullyProcessedRound(replicationLogFileTracker);
    }

    protected void initLastSuccessfullyProcessedRound(final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
//        // First check in-progress directory
//        List<Path> inProgressFiles = replicationLogFileTracker.getInProgressFiles();
//        if (!inProgressFiles.isEmpty()) {
//            long minTimestamp = getMinTimestampFromFiles(replicationLogFileTracker, inProgressFiles);
//            this.lastSuccessfullyProcessedRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestamp);
//        }
//
//        // If no in-progress files, check IN directory
//        // Get files from all shard directories in the IN directory
//        List<Path> inFiles = replicationLogFileTracker.getNewFiles();
//        if (!inFiles.isEmpty()) {
//            long minTimestamp = getMinTimestampFromFiles(replicationLogFileTracker, inFiles);
//            this.lastSuccessfullyProcessedRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestamp);
//        }
//
//        // If no files found, set it to current time
//        this.lastSuccessfullyProcessedRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime());

        this.lastSuccessfullyProcessedRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime() - 5*replicationLogFileTracker.getReplicationShardDirectoryManager().getRoundTimeSeconds()*1000L);
    }

    private long getMinTimestampFromFiles(ReplicationLogFileTracker replicationLogFileTracker, List<Path> files) {
        long minTimestamp = Long.MAX_VALUE;
        for (Path file : files) {
            minTimestamp = Math.min(minTimestamp, replicationLogFileTracker.getFileTimestamp(file));
        }
        return minTimestamp;
    }

    public Round getLastSuccessfullyProcessedRound() {
        return lastSuccessfullyProcessedRound;
    }
}
