package org.apache.phoenix.replication.reader;

public class ReplicationStateTracker {

    private final Round previousFinishedRound;

    public ReplicationStateTracker(Round previousFinishedRound) {
        this.previousFinishedRound = previousFinishedRound;
    }

    public Round getPreviousFinishedRound() {
        return previousFinishedRound;
    }
}
