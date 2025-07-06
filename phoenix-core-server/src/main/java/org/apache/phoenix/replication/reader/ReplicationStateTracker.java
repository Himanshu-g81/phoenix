package org.apache.phoenix.replication.reader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.common.ReplicationShardDirectoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ReplicationStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationStateTracker.class);

    private Round previousFinishedRound;

    protected ReplicationStateTracker(final Configuration conf, final String haGroupName) {

    }

    protected void init(ReplicationLogFileTracker replicationLogFileTracker) {
        // Initialize the previous finished round object
        previousFinishedRound = replicationLogFileTracker.getLastSuccessfullyProcessedRound();
    }

    public Round getPreviousFinishedRound() {
        return previousFinishedRound;
    }
}
