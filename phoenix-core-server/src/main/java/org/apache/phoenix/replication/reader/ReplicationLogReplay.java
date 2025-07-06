package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.common.ReplicationShardDirectoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReplicationLogReplay {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogReplay.class);

    private Configuration conf;
    private String haGroupId;
    private ReplicationLogFileTracker replicationLogFileTracker;
    private ReplicationStateTracker replicationStateTracker;

    public ReplicationLogReplay(final Configuration conf, final String haGroupId) {
        this.conf = conf;
        this.haGroupId = haGroupId;
    }

    public void start() throws IOException {
        // Start a scheduler service with random delay in between two process (1 - 10 seconds)
        // TODO: Add a state check that scheduler is not already running
        init();
        startScheduler();
    }

    protected void init() throws IOException {
        // Initialize file system
        this.replicationLogFileTracker = new ReplicationLogFileTracker(conf, haGroupId);
        this.replicationStateTracker = new ReplicationStateTracker(conf, haGroupId);
        this.replicationLogFileTracker.init();
        this.replicationStateTracker.init(replicationLogFileTracker);
    }

    protected void startScheduler() {
        System.out.println("String Scheduler for " + haGroupId);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                replay();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, new Random().nextInt(10), 10, TimeUnit.SECONDS);
    }

    public void replay() throws IOException {
        System.out.println("Starting replay");
        List<Round> roundList = getRoundsToProcess();
        System.out.println("Number of rounds to process: " + roundList.size());
        for(Round round : roundList) {
            processRound(round);
        }
    }

    public List<Round> getRoundsToProcess() {
        long currentTime = System.currentTimeMillis();
        long previousRoundEndTime = replicationStateTracker.getPreviousFinishedRound().getEndTime();
        long roundTime = conf.getLong(ReplicationShardDirectoryManager.REPLICATION_ROUND_TIME_PERIOD_SECONDS_KEY, ReplicationShardDirectoryManager.DEFAULT_REPLICATION_ROUND_TIME_PERIOD_SECONDS) * 1000L;
        long bufferSeconds = 5000;
        final List<Round> rounds = new ArrayList<>();
        // If currentTime > previous time + round time + buffer
        for(long startTime = previousRoundEndTime; startTime < currentTime - roundTime - bufferSeconds; startTime += roundTime) {
            rounds.add(new Round(startTime, startTime + roundTime));
        }
        return rounds;
    }

    public void processRound(Round round) throws IOException {
        System.out.println("Starting to process round: startTime:" + round.getStartTime() + " and endTime: " + round.getEndTime());
        // Process IN directory for a round
        List<Path> files = replicationLogFileTracker.getNewFilesForRound(round);
        while(!files.isEmpty()) {
            // Pick a random file and process it
            Path file = files.get(new Random().nextInt(files.size()));
            try {
                System.out.println("Applying file: " + file.toString());
                ReplicationLogProcessor.get(conf, haGroupId).processLogFile(replicationLogFileTracker.getFileSystem(), file);
                replicationLogFileTracker.markCompleted(file);
            } catch (IOException exception) {
                // Log the error
            }
            files = replicationLogFileTracker.getNewFilesForRound(round);
        }
        if(shouldProcessInProgressDirectory()) {
            processInProgressDirectory();
        }
    }

    protected boolean shouldProcessInProgressDirectory() {
        return new Random().nextInt(10) == 0;
    }

    protected void processInProgressDirectory() throws IOException {
        List<Path> files = replicationLogFileTracker.getInProgressFiles();
        while (!files.isEmpty()) {

        }
        for(Path file : files) {
            // mark file in progress
            boolean status = replicationLogFileTracker.markInProgress(file);
            if(status) {
                try {
                    ReplicationLogProcessor.get(conf, haGroupId).processLogFile(replicationLogFileTracker.getFileSystem(), file);
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
