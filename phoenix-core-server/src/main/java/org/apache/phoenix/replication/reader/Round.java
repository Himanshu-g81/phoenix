package org.apache.phoenix.replication.reader;

import com.google.common.base.Preconditions;

public class Round {

    private final long startTime;
    private final long endTime;

    public Round(long startTime, long endTime) {
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
