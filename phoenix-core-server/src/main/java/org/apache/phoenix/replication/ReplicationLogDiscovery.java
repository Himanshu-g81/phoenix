package org.apache.phoenix.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public abstract class ReplicationLogDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscovery.class);

    private static final int DEFAULT_EXECUTOR_THREAD_COUNT = 1;

    private static final String DEFAULT_EXECUTOR_THREAD_NAME_FORMAT = "ReplicationLogDiscovery-%d";

    private static final long DEFAULT_REPLAY_INTERVAL_SECONDS = 60;

    private static final long DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;

    private static final double DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY = 5.0;

    private static final double DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0;

    private final Configuration conf;
    private final String haGroupName;
    private final ReplicationLogFileTracker replicationLogFileTracker;
    private final ReplicationStateTracker replicationStateTracker;
    private ScheduledExecutorService scheduler;
    private volatile boolean isRunning = false;

    public ReplicationLogDiscovery(final ReplicationLogFileTracker replicationLogFileTracker, final ReplicationStateTracker replicationStateTracker) {
        this.replicationLogFileTracker = replicationLogFileTracker;
        this.replicationStateTracker = replicationStateTracker;
        this.haGroupName = replicationLogFileTracker.getHaGroupName();
        this.conf = replicationLogFileTracker.getConf();
    }

    public void start() throws IOException {
        synchronized (this) {
            if (isRunning) {
                LOG.warn("ReplicationLogDiscovery is already running for group: {}", haGroupName);
                return;
            }
            // Initialize and schedule the executors
            scheduler = Executors.newScheduledThreadPool(getExecutorThreadCount(), new ThreadFactoryBuilder()
                    .setNameFormat(getExecutorThreadNameFormat()).build());
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    replay();
                } catch (IOException e) {
                    LOG.error("Error during replay", e);
                }
            }, 0, getReplayIntervalSeconds(), TimeUnit.SECONDS);
            
            isRunning = true;
            LOG.info("ReplicationLogDiscovery started for group: {}", haGroupName);
        }
    }

    public void stop() throws IOException {
        ScheduledExecutorService schedulerToShutdown = null;
        
        synchronized (this) {
            if (!isRunning) {
                LOG.warn("ReplicationLogDiscovery is not running for group: {}", haGroupName);
                return;
            }
            
            isRunning = false;
            schedulerToShutdown = scheduler;
        }

        if (schedulerToShutdown != null && !schedulerToShutdown.isShutdown()) {
            schedulerToShutdown.shutdown();
            try {
                if (!schedulerToShutdown.awaitTermination(getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                    schedulerToShutdown.shutdownNow();
                }
            } catch (InterruptedException e) {
                schedulerToShutdown.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        LOG.info("ReplicationLogDiscovery stopped for group: {}", haGroupName);
    }

    public void replay() throws IOException {
        System.out.println("Starting replay");
        List<ReplicationRound> replicationRoundList = getRoundsToProcess();
        System.out.println("Number of rounds to process: " + replicationRoundList.size());
        for(ReplicationRound replicationRound : replicationRoundList) {
            processRound(replicationRound);
        }
    }

    protected List<ReplicationRound> getRoundsToProcess() {
        long currentTime = System.currentTimeMillis();
        long previousRoundEndTime = replicationStateTracker.getLastSuccessfullyProcessedRound().getEndTime();
        long roundTimeMills = replicationLogFileTracker.getReplicationShardDirectoryManager().getRoundTimeSeconds() * 1000L;
        long bufferMillis = (long) (roundTimeMills * getWaitingBufferPercentage() / 100.0);
        final List<ReplicationRound> replicationRounds = new ArrayList<>();
        for(long startTime = previousRoundEndTime; startTime < currentTime - roundTimeMills - bufferMillis; startTime += roundTimeMills) {
            replicationRounds.add(replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromStartTime(startTime));
        }
        return replicationRounds;
    }

    protected void processRound(ReplicationRound replicationRound) throws IOException {
        System.out.println("Starting to process round: startTime:" + replicationRound.getStartTime() + " and endTime: " + replicationRound.getEndTime());
        // Process IN directory for a round
        processNewFilesForRound(replicationRound);
        if(shouldProcessInProgressDirectory()) {
            processInProgressDirectory();
        }
    }

    protected boolean shouldProcessInProgressDirectory() {
        return ThreadLocalRandom.current().nextDouble(100.0) < getInProgressDirectoryProcessProbability();
    }

    protected void processNewFilesForRound(ReplicationRound replicationRound) throws IOException {
        List<Path> files = replicationLogFileTracker.getNewFilesForRound(replicationRound);
        System.out.println("Number of new files for round: " + files.size());
        while(!files.isEmpty()) {
            // Pick a random file and process it
            Path file = files.get(new Random().nextInt(files.size()));
            try {
                processFile(file);
                replicationLogFileTracker.markCompleted(file);
            } catch (IOException exception) {
                // Log the error
            }
            files = replicationLogFileTracker.getNewFilesForRound(replicationRound);
        }
    }

    protected void processInProgressDirectory() throws IOException {
        List<Path> files = replicationLogFileTracker.getInProgressFiles();
        for(Path file : files) {
            // mark file in progress
            boolean status = replicationLogFileTracker.markInProgress(file);
            if(status) {
                // Start processing the file only if it was marked in-progress successfully
                try {
                    processFile(file);
                    replicationLogFileTracker.markCompleted(file);
                } catch (IOException exception) {
                    replicationLogFileTracker.markFileAsFailed(file);
                }
            }
        }
    }

    protected abstract void processFile(Path path) throws IOException;

    protected ReplicationStateTracker getReplicationStateTracker() {
        return this.replicationStateTracker;
    }

    protected ReplicationLogFileTracker getReplicationLogFileTracker() {
        return this.replicationLogFileTracker;
    }

    protected Configuration getConf() {
        return this.conf;
    }

    protected String getHaGroupName() {
        return this.haGroupName;
    }

    protected int getExecutorThreadCount() {
        return DEFAULT_EXECUTOR_THREAD_COUNT;
    }

    protected String getExecutorThreadNameFormat() {
        return DEFAULT_EXECUTOR_THREAD_NAME_FORMAT;
    }

    /**
     * Returns the replay interval in seconds. Subclasses can override this method to provide custom intervals.
     * @return The replay interval in seconds
     */
    protected long getReplayIntervalSeconds() {
        return DEFAULT_REPLAY_INTERVAL_SECONDS;
    }

    /**
     * Returns the shutdown timeout in seconds. Subclasses can override this method to provide custom timeout values.
     * @return The shutdown timeout in seconds
     */
    protected long getShutdownTimeoutSeconds() {
        return DEFAULT_SHUTDOWN_TIMEOUT_SECONDS;
    }

    protected double getInProgressDirectoryProcessProbability() {
        return DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY;
    }

    /**
     * Returns the buffer percentage for calculating buffer time. Subclasses can override this method 
     * to provide custom buffer percentages.
     * @return The buffer percentage (default 15.0%)
     */
    protected double getWaitingBufferPercentage() {
        return DEFAULT_WAITING_BUFFER_PERCENTAGE;
    }

}
