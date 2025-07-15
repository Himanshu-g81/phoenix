package org.apache.phoenix.replication;

import com.google.common.base.Preconditions;

public class ReplicationRound {

    private final long startTime;
    private final long endTime;

    public ReplicationRound(long startTime, long endTime) {
        Preconditions.checkArgument(startTime < endTime);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }
}
