package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.common.ReplicationContext;

import java.util.Collections;
import java.util.List;

public class ReplicationLogFileTracker {

    private Configuration conf;
    private ReplicationContext replicationContext;

    public ReplicationLogFileTracker(final Configuration conf, final ReplicationContext replicationContext) {
        this.conf = conf;
        this.replicationContext = replicationContext;
    }

    public List<Path> getNewFilesForRound(Round round) {
        Path roundDirectory = replicationContext.getInRoundDirectory(round);
        // list files in round directory, filter the once within round start and end timestamp
        // return list of files
        return Collections.emptyList();
    }

    public List<Path> getInProgressFiles() {
        return Collections.emptyList();
    }

    public boolean markCompleted(final Path file) {
        return false;
    }

    public boolean markInProgress(final Path file) {
        return false;
    }

    public boolean markFileAsFailed(final Path file) {
        return false;
    }
}
