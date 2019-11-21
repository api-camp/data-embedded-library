package io.github.de314.ac.data.utils.metrics;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Timer {

    private long startTime = System.currentTimeMillis();
    private Long endTime;

    public void start() {
        this.startTime = System.currentTimeMillis();
    }

    public void stop() {
        this.endTime = System.currentTimeMillis();
    }

    public long getDuration() {
        return (this.endTime != null ? endTime : System.currentTimeMillis()) - this.startTime;
    }

    public double getRatePerSecond(long count) {
        return count * 1000.0 / getDuration();
    }

    public String toString() {
        return String.format("TIMER(duration=%,d ms)", getDuration());
    }

    public String toString(long count) {
        return String.format("TIMER(duration=%,d ms rate=%,.2f ops/sec)", getDuration(), getRatePerSecond(count));
    }

    public static Timer create() {
        return new Timer();
    }
}
