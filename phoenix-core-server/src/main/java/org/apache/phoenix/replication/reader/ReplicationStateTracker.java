package org.apache.phoenix.replication.reader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.common.ReplicationShardDirectoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationStateTracker.class);
    
    // Singleton instances per haGroupName
    private static final Map<String, ReplicationStateTracker> instances = new ConcurrentHashMap<>();

    private final Configuration conf;
    private final String haGroupName;
    private Round previousFinishedRound;

    private ReplicationStateTracker(final Configuration conf, final String haGroupName) {
        this.conf = conf;
        this.haGroupName = haGroupName;
    }
    
    /**
     * Gets the singleton instance of ReplicationStateTracker for the given haGroupName.
     * Creates a new instance if one doesn't exist for the haGroupName.
     * 
     * @param conf Configuration object
     * @param haGroupName The HA group name
     * @return The singleton ReplicationStateTracker instance for the haGroupName
     */
    public static ReplicationStateTracker get(Configuration conf, String haGroupName) {
        return instances.computeIfAbsent(haGroupName, groupName -> {
            ReplicationStateTracker tracker = new ReplicationStateTracker(conf, groupName);
            try {
                tracker.init(ReplicationLogFileTracker.get(conf, haGroupName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return tracker;
        });
    }

    protected void init(ReplicationLogFileTracker replicationLogFileTracker) {
        // Initialize the previous finished round object
        previousFinishedRound = replicationLogFileTracker.getLastSuccessfullyProcessedRound();
    }

    public Round getPreviousFinishedRound() {
        return previousFinishedRound;
    }
}
