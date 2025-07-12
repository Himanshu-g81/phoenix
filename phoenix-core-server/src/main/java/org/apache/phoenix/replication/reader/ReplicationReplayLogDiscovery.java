package org.apache.phoenix.replication.reader;

import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.ReplicationLogDiscovery;
import org.apache.phoenix.replication.ReplicationStateTracker;

import java.io.IOException;

public class ReplicationReplayLogDiscovery extends ReplicationLogDiscovery {

    private static final String EXECUTOR_THREAD_NAME_FORMAT = "Phoenix-Replication-Replay-%d";

    public ReplicationReplayLogDiscovery(final ReplicationLogReplayFileTracker replicationLogReplayFileTracker, final ReplicationStateTracker replicationStateTracker) {
        super(replicationLogReplayFileTracker, replicationStateTracker);
    }

    @Override
    protected void processFile(Path path) throws IOException {
        System.out.println("Processing file " + path);
        ReplicationLogProcessor.get(getConf(), getHaGroupName()).processLogFile(getReplicationLogFileTracker().getFileSystem(), path);
    }

    @Override
    protected String getExecutorThreadNameFormat() {
        return EXECUTOR_THREAD_NAME_FORMAT;
    }
}
