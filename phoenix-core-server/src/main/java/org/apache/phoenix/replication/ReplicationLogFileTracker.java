package org.apache.phoenix.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.common.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.reader.Round;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ReplicationLogFileTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogFileTracker.class);
    
    // Singleton instances per haGroupName
    private static final Map<String, ReplicationLogFileTracker> instances = new ConcurrentHashMap<>();
    
    // Configuration keys for file operations
    private static final String FILE_DELETE_RETRIES_KEY = "phoenix.replication.file.delete.retries";
    private static final int DEFAULT_FILE_DELETE_RETRIES = 3;
    private static final String FILE_DELETE_RETRY_DELAY_MS_KEY = "phoenix.replication.file.delete.retry.delay.ms";
    private static final long DEFAULT_FILE_DELETE_RETRY_DELAY_MS = 1000L;

    private final Configuration conf;
    private final String haGroupName;
    private final URI rootURI;
    private final FileSystem fileSystem;
    private Path inProgressDirPath;
    private ReplicationShardDirectoryManager replicationShardDirectoryManager;

    protected ReplicationLogFileTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) {
        this.conf = conf;
        this.fileSystem = fileSystem;
        this.haGroupName = haGroupName;
        this.rootURI = rootURI;
        Path newFilesDirectory = new Path(new Path(rootURI.getPath(), getNewLogSubDirectoryName()), haGroupName);
        this.replicationShardDirectoryManager = new ReplicationShardDirectoryManager(conf, newFilesDirectory);
        this.inProgressDirPath = new Path(new Path(rootURI.getPath(), getInProgressLogSubDirectoryName()), this.haGroupName);
    }

    protected abstract String getNewLogSubDirectoryName();

    protected String getInProgressLogSubDirectoryName() {
        return getNewLogSubDirectoryName() + "_progress";
    }

    public void init() throws IOException {
        createDirectoryIfNotExists(inProgressDirPath);
    }

    protected void createDirectoryIfNotExists(Path directoryPath) throws IOException {
        if (!fileSystem.exists(directoryPath)) {
            LOG.info("Creating directory {}", directoryPath);
            if (!fileSystem.mkdirs(directoryPath)) {
                throw new IOException("Failed to create directory: " + directoryPath);
            }
        }
    }

    public List<Path> getNewFilesForRound(Round round) throws IOException {
        Path roundDirectory = replicationShardDirectoryManager.getShardDirectory(round);
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
            if(status.isFile()) {
                if (!isValidLogFile(status.getPath())) {
                    LOG.debug("Invalid log files found at " + status.getPath());
                    continue; // Skip invalid non-log files
                }
                try {
                    long fileTimestamp = getFileTimestamp(status.getPath());
                    if(fileTimestamp >= round.getStartTime() && fileTimestamp <= round.getEndTime()) {
                        filesInRound.add(status.getPath());
                    }
                } catch (NumberFormatException exception) {
                    // Should we throw an exception here instead?
                    LOG.warn("Failed to extract timestamp from {}. Ignoring the file.", status.getPath());
                }
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
            if (status.isFile() && isValidLogFile(status.getPath())) {
                inProgressFiles.add(status.getPath());
            }
        }

        return inProgressFiles;
    }

    public List<Path> getNewFiles() throws IOException {
        List<Path> shardPaths = replicationShardDirectoryManager.getAllShardPaths();
        List<Path> newFiles = new ArrayList<>();
        for(Path shardPath : shardPaths) {
            FileStatus[] fileStatuses = fileSystem.listStatus(shardPath);
            for(FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile() && isValidLogFile(fileStatus.getPath())) {
                    newFiles.add(fileStatus.getPath());
                }
            }
        }
        return newFiles;
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
            
            // Sleep in case of failure and it's not the last attempt
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
            Optional<String> optionalUUID = getFileUUID(file);
            if(optionalUUID.isPresent()) {
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

    public boolean isValidLogFile(Path path) throws IOException {
        final String fileName = path.getName();
        if (!fileName.endsWith(".plog")) {
            return false;
        }
        return LogFile.isValidLogFile(fileSystem, path);
    }

    public long getFileTimestamp(Path path) throws NumberFormatException {
        String[] parts = path.getName().split("_");
        return Long.parseLong(parts[0]);
    }

    public Optional<String> getFileUUID(Path path) throws NumberFormatException {
        String[] parts = path.getName().split("_");
        if(parts.length < 3) {
            return Optional.empty();
        }
        return Optional.of(parts[parts.length-1]);
    }

    public boolean markFileAsFailed(final Path file) {
        return true;
    }

    public FileSystem getFileSystem() {
        return this.fileSystem;
    }

    public ReplicationShardDirectoryManager getReplicationShardDirectoryManager() {
        return this.replicationShardDirectoryManager;
    }

    public String getHaGroupName() {
        return this.haGroupName;
    }

    public Configuration getConf() {
        return this.conf;
    }
}
