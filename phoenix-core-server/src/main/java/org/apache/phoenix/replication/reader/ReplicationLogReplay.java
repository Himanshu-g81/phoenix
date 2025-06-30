package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.phoenix.replication.common.ReplicationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReplicationLogReplay {

    private Configuration conf;
    private String haGroupId;
    private final ReplicationLogFileTracker replicationLogFileTracker;
    private final ReplicationStateTracker replicationStateTracker;
    private final ReplicationContext replicationContext;

    public ReplicationLogReplay(final Configuration conf, final String haGroupId, final ReplicationLogFileTracker replicationLogFileTracker, ReplicationStateTracker replicationStateTracker) {
        this.conf = conf;
        this.haGroupId = haGroupId;
        this.replicationLogFileTracker = replicationLogFileTracker;
        this.replicationStateTracker = replicationStateTracker;
        this.replicationContext = new ReplicationContext(conf);
    }

    public void start() {
        // Start a scheduler service with random delay in between two process (1 - 10 seconds)
    }

    public void replay() {
        List<Round> roundList = getRoundsToProcess();
        for(Round round : roundList) {
            processRound(round);
        }
    }

    public List<Round> getRoundsToProcess() {
        long currentTime = System.currentTimeMillis();
        long previousRoundEndTime = replicationStateTracker.getPreviousFinishedRound().getEndTime();
        long roundTime = 1;
        long bufferSeconds = 100;
        final List<Round> rounds = new ArrayList<>();
        // If currentTime > previous time + round time + buffer
        for(long startTime = previousRoundEndTime; startTime < currentTime + roundTime + bufferSeconds; startTime += roundTime) {
            rounds.add(new Round(startTime, startTime + roundTime));
        }
        return rounds;
    }

    public void processRound(Round round) {
        List<Path> files = replicationLogFileTracker.getNewFilesForRound(round);
        processFiles(files);
        if(shouldProcessInProgressDirectory()) {
            processFiles(replicationLogFileTracker.getInProgressFiles());
        }
    }

    protected boolean shouldProcessInProgressDirectory() {
        return new Random().nextInt(10) == 0;
    }

    protected void processFiles(List<Path> files) {
        for(Path file : files) {
            // mark file in progress
            boolean status = replicationLogFileTracker.markInProgress(file);
            if(status) {
                try {
                    ReplicationLogProcessor.get(conf, haGroupId).processLogFile(replicationContext.getStandbyFs(), file);
                    replicationLogFileTracker.markCompleted(file);
                } catch (IOException exception) {
                    replicationLogFileTracker.markFileAsFailed(file);
                }
            } else {
                // Log that file in progress failed, so skipping the file
            }
        }
    }
}
