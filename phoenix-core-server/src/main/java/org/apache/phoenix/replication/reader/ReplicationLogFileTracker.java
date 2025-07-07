package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.common.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.log.LogFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationLogFileTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogFileTracker.class);
    
    // Singleton instances per haGroupName
    private static final Map<String, ReplicationLogFileTracker> instances = new ConcurrentHashMap<>();

    public static final String IN = "IN";
    public static final String IN_PROGRESS = "IN_PROGRESS";
    
    // Configuration keys for file operations
    private static final String FILE_DELETE_RETRIES_KEY = "phoenix.replication.file.delete.retries";
    private static final int DEFAULT_FILE_DELETE_RETRIES = 3;
    private static final String FILE_DELETE_RETRY_DELAY_MS_KEY = "phoenix.replication.file.delete.retry.delay.ms";
    private static final long DEFAULT_FILE_DELETE_RETRY_DELAY_MS = 1000L;

    private final Configuration conf;
    private final String haGroupName;
    private FileSystem fileSystem;
    private Path haGroupDirPath;
    private Path inDirPath;
    private Path inProgressDirPath;
    private ReplicationShardDirectoryManager replicationShardDirectoryManager;

    private ReplicationLogFileTracker(final Configuration conf, final String haGroupName) {
        this.conf = conf;
        this.haGroupName = haGroupName;
    }
    
    /**
     * Gets the singleton instance of ReplicationLogFileTracker for the given haGroupName.
     * Creates a new instance if one doesn't exist for the haGroupName.
     * 
     * @param conf Configuration object
     * @param haGroupName The HA group name
     * @return The singleton ReplicationLogFileTracker instance for the haGroupName
     * @throws IOException If initialization fails
     */
    public static ReplicationLogFileTracker get(Configuration conf, String haGroupName) throws IOException {
        return instances.computeIfAbsent(haGroupName, name -> {
            try {
                ReplicationLogFileTracker tracker = new ReplicationLogFileTracker(conf, name);
                tracker.init();
                return tracker;
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize ReplicationLogFileTracker for haGroup: " + name, e);
            }
        });
    }

    public void init() throws IOException {
        // Create in and in-progress directories
        createReplayDirectories();
        this.replicationShardDirectoryManager = new ReplicationShardDirectoryManager(conf, inDirPath);
    }

    protected void createReplayDirectories() throws IOException {
        String uriString = conf.get(ReplicationLogReplayService.REPLICATION_LOG_REPLAY_HDFS_URL_KEY);
        if (uriString == null) {
            throw new IOException(ReplicationLogReplayService.REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not configured");
        }
        try {
            URI rootURI = new URI(uriString);
            this.fileSystem = FileSystem.get(rootURI, conf);
            this.haGroupDirPath = new Path(rootURI.getPath(), haGroupName);
            // Create HA Group directory
            if (!fileSystem.exists(haGroupDirPath)) {
                LOG.info("Creating directory {}", haGroupDirPath);
                if (!fileSystem.mkdirs(haGroupDirPath)) {
                    throw new IOException("Failed to create directory: " + haGroupDirPath);
                }
            }
            // Create IN Directory
            this.inDirPath = new Path(haGroupDirPath, IN);
            if (!fileSystem.exists(inDirPath)) {
                LOG.info("Creating directory {}", inDirPath);
                if (!fileSystem.mkdirs(inDirPath)) {
                    throw new IOException("Failed to create directory: " + inDirPath);
                }
            }
            // Create IN PROGRESS Directory
            this.inProgressDirPath = new Path(haGroupDirPath, IN_PROGRESS);
            if (!fileSystem.exists(inProgressDirPath)) {
                LOG.info("Creating directory {}", inProgressDirPath);
                if (!fileSystem.mkdirs(inProgressDirPath)) {
                    throw new IOException("Failed to create directory: " + inProgressDirPath);
                }
            }
        } catch (URISyntaxException e) {
            throw new IOException(ReplicationLogReplayService.REPLICATION_LOG_REPLAY_HDFS_URL_KEY  + " is not valid", e);
        }
    }

    public List<Path> getNewFiles() throws IOException {
        // List the files in roundDirectory
        FileStatus[] fileStatuses = fileSystem.listStatus(inDirPath);
        List<Path> filesInRound = new ArrayList<>();

        // Filter the files belonging to current round
        for (FileStatus status : fileStatuses) {
            if (!status.isFile()) {
                continue; // Skip directories
            }

            String fileName = status.getPath().getName();
            if (!fileName.endsWith(".plog")) {
                continue; // Skip non-log files
            }

            // Extract timestamp from file name (first part before underscore)
            String[] parts = fileName.split("_");
            if (parts.length == 0) {
                continue; // Skip files without proper naming
            }

            filesInRound.add(status.getPath());
        }

        return filesInRound;
    }

    public List<Path> getNewFilesForRound(Round round) throws IOException {
        Path roundDirectory = replicationShardDirectoryManager.getShardDirectory(round.getStartTime());
        System.out.println("Getting new files for round: " + round.getStartTime() + " - " + roundDirectory.toString());
        if (!fileSystem.exists(roundDirectory)) {
            return Collections.emptyList();
        }

        // List the files in roundDirectory
        FileStatus[] fileStatuses = fileSystem.listStatus(roundDirectory);
        System.out.println("Number of files found " + fileStatuses.length);
        List<Path> filesInRound = new ArrayList<>();

        // Filter the files belonging to current round
        for (FileStatus status : fileStatuses) {
            if (!status.isFile()) {
                continue; // Skip directories
            }
            
            String fileName = status.getPath().getName();
            if (!fileName.endsWith(".plog")) {
                continue; // Skip non-log files
            }
            
            // Extract timestamp from file name (first part before underscore)
            String[] parts = fileName.split("_");
            if (parts.length == 0) {
                parts = fileName.split("-");
            }
            if (parts.length == 0) {
                continue; // Skip files without proper naming
            }
            System.out.println("Getting timestamp from file: " + fileName + " as " + parts[0]);
            try {
                long fileTimestamp = Long.parseLong(parts[0]);
                
                // Check if timestamp is between round start and end time (end time is exclusive)
                if (fileTimestamp >= round.getStartTime() && fileTimestamp < round.getEndTime()) {
                    filesInRound.add(status.getPath());
                }
            } catch (NumberFormatException e) {
                // Skip files with invalid timestamp format
                LOG.warn("Skipping file with invalid timestamp format: {}", fileName);
            }
        }
        
        return filesInRound;
    }

    public List<Path> getInProgressFiles() throws IOException {
        if (!fileSystem.exists(inProgressDirPath)) {
            return Collections.emptyList();
        }

        FileStatus[] fileStatuses = fileSystem.listStatus(inProgressDirPath);
        List<Path> inProgressFiles = new ArrayList<>();
        
        for (FileStatus status : fileStatuses) {
            if (status.isFile() && status.getPath().getName().endsWith(".plog")) {
                inProgressFiles.add(status.getPath());
            }
        }
        
        return inProgressFiles;
    }

    public boolean markCompleted(final Path file) {
        System.out.println("Mark Completed Method Called for " + file.toString());
        int maxRetries = conf.getInt(FILE_DELETE_RETRIES_KEY, DEFAULT_FILE_DELETE_RETRIES);
        long retryDelayMs = conf.getLong(FILE_DELETE_RETRY_DELAY_MS_KEY, DEFAULT_FILE_DELETE_RETRY_DELAY_MS);
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (fileSystem.delete(file, false)) {
                    System.out.println("Successfully deleted completed file: " + file);
                    LOG.debug("Successfully deleted completed file: {}", file);
                    return true;
                } else {
                    LOG.warn("Failed to delete file (attempt {}): {}", attempt + 1, file);
                }
            } catch (IOException e) {
                LOG.warn("IOException while deleting file (attempt {}): {}", attempt + 1, file, e);
            }
            
            // Don't sleep on the last attempt
            if (attempt < maxRetries) {
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting to retry file deletion: {}", file);
                    return false;
                }
            }
        }
        
        LOG.error("Failed to delete file after {} attempts: {}", maxRetries + 1, file);
        return false;
    }

    public boolean markInProgress(final Path file) {
        System.out.println("Mark In Progress Method Called for " + file.toString());
        try {
            String fileName = file.getName();
            String newFileName;
            Path targetDirectory;
            
            // Check if file already has a UUID suffix
            if (fileName.contains("_") && fileName.split("_").length >= 3) {
                // File already has UUID, replace it with a new one (stay in same directory)
                String[] parts = fileName.split("_");
                // Remove the last part (UUID) and add new UUID
                StringBuilder newNameBuilder = new StringBuilder();
                for (int i = 0; i < parts.length - 1; i++) {
                    if (i > 0) {
                        newNameBuilder.append("_");
                    }
                    newNameBuilder.append(parts[i]);
                }
                newNameBuilder.append("_").append(UUID.randomUUID().toString());
                newFileName = newNameBuilder.toString();
                targetDirectory = file.getParent();
            } else {
                // File doesn't have UUID, add one and move to IN_PROGRESS directory
                String baseName = fileName.substring(0, fileName.lastIndexOf("."));
                String extension = fileName.substring(fileName.lastIndexOf("."));
                newFileName = baseName + "_" + UUID.randomUUID().toString() + extension;
                targetDirectory = inProgressDirPath;
            }
            
            Path newPath = new Path(targetDirectory, newFileName);
            
            if (fileSystem.rename(file, newPath)) {
                LOG.debug("Successfully marked file as in progress: {} -> {}", file.getName(), newFileName);
                return true;
            } else {
                LOG.warn("Failed to rename file for in-progress marking: {}", file);
                return false;
            }
        } catch (IOException e) {
            LOG.error("IOException while marking file as in progress: {}", file, e);
            return false;
        }
    }

    public Round getLastSuccessfullyProcessedRound() {
        try {
            // First check in-progress directory
            List<Path> inProgressFiles = getInProgressFiles();
            if (!inProgressFiles.isEmpty()) {
                long minTimestamp = getMinTimestampFromFiles(inProgressFiles);
                long roundEndTime = replicationShardDirectoryManager.getNearestRoundStartTimestamp(minTimestamp);
                long roundStartTime = roundEndTime - (replicationShardDirectoryManager.getRoundTimeSeconds() * 1000L);
                LOG.info("Initialized previous finished round from in-progress files: {} - {}", roundStartTime, roundEndTime);
                return new Round(roundStartTime, roundEndTime);
            }

            // If no in-progress files, check IN directory
            // Get files from all shard directories in the IN directory
            List<Path> inFiles = getNewFiles();
            if (!inFiles.isEmpty()) {
                long minTimestamp = getMinTimestampFromFiles(inFiles);
                long roundEndTime = replicationShardDirectoryManager.getNearestRoundStartTimestamp(minTimestamp);
                long roundStartTime = roundEndTime - (replicationShardDirectoryManager.getRoundTimeSeconds() * 1000L);
                LOG.info("Initialized previous finished round from IN files: {} - {}", roundStartTime, roundEndTime);
                return new Round(roundStartTime, roundEndTime);
            }

            // If no files found, set to a default round
            long currentTime = System.currentTimeMillis() - 5*replicationShardDirectoryManager.getRoundTimeSeconds()*1000;
            long roundEndTime = replicationShardDirectoryManager.getNearestRoundStartTimestamp(currentTime);
            long roundStartTime = roundEndTime - (replicationShardDirectoryManager.getRoundTimeSeconds() * 1000L);
            LOG.info("No files found, initialized previous finished round to current time: {} - {}", roundStartTime, roundEndTime);
            return new Round(roundStartTime, roundEndTime);

        } catch (Exception e) {
            LOG.error("Error initializing previous finished round", e);
            // Set a default round in case of error
            long currentTime = System.currentTimeMillis() - 3*replicationShardDirectoryManager.getRoundTimeSeconds()*1000;;
            long roundEndTime = replicationShardDirectoryManager.getNearestRoundStartTimestamp(currentTime);
            long roundStartTime = roundEndTime - (replicationShardDirectoryManager.getRoundTimeSeconds() * 1000L);
            return new Round(roundStartTime, roundEndTime);
        }
    }

    private long getMinTimestampFromFiles(List<Path> files) {
        long minTimestamp = Long.MAX_VALUE;
        for (Path file : files) {
            String fileName = file.getName();
            if (fileName.endsWith(".plog")) {
                // Extract timestamp from file name (first part before underscore or hyphen)
                String[] parts = fileName.split("_");
                if (parts.length == 0) {
                    parts = fileName.split("-");
                }
                if (parts.length > 0) {
                    System.out.println("Analysis file: " + fileName + " - timestamp as " + parts[0]);
                    try {
                        long fileTimestamp = Long.parseLong(parts[0]);
                        if (fileTimestamp < minTimestamp) {
                            minTimestamp = fileTimestamp;
                        }
                    } catch (NumberFormatException e) {
                        LOG.warn("Skipping file with invalid timestamp format: {}", fileName);
                    }
                }
            }
        }
        return minTimestamp == Long.MAX_VALUE ? System.currentTimeMillis() : minTimestamp;
    }


    public boolean markFileAsFailed(final Path file) {
        return true;
    }

    public FileSystem getFileSystem() {
        return this.fileSystem;
    }
}
