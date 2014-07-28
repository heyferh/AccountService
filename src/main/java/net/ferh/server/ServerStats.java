package net.ferh.server;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ferh on 27.07.14.
 */
public class ServerStats {

    private AtomicLong startTime;
    private AtomicLong totalReadQueries;
    private AtomicLong totalWriteQueries;
    private PrintWriter printWriter;

    public AtomicLong getStartTime() {
        return startTime;
    }

    public long getTotalWriteQueries() {
        return totalWriteQueries.get();
    }

    public long getTotalReadQueries() {
        return totalReadQueries.get();
    }

    public void setTotalReadQueries(AtomicLong totalReadQueries) {
        this.totalReadQueries.set(totalReadQueries.get());

    }

    public void setStartTime(AtomicLong startTime) {
        this.startTime.set(startTime.get());
    }

    public void setTotalWriteQueries(AtomicLong totalWriteQueries) {
        this.totalWriteQueries.set(totalWriteQueries.get());
    }

    public ServerStats() {
        startTime = new AtomicLong(System.currentTimeMillis());
        totalReadQueries = new AtomicLong(0);
        totalWriteQueries = new AtomicLong(0);
    }

    public void incrementReadQueries() {
        totalReadQueries.incrementAndGet();
    }

    public void incrementWriteQueries() {
        totalWriteQueries.incrementAndGet();
    }

    public long getReadRPS() {
        final long start = startTime.get();
        final long totalRunningTime = System.currentTimeMillis() - start;
        final long readQueries = totalReadQueries.get();
        return 1000 * readQueries / totalRunningTime;
    }

    public long getWriteRPS() {
        final long start = startTime.get();
        final long totalRunningTime = System.currentTimeMillis() - start;
        final long writeQueries = totalWriteQueries.get();
        return 1000 * writeQueries / totalRunningTime;
    }

    public long getTotalRPS() {
        final long start = startTime.get();
        final long totalRunningTime = System.currentTimeMillis() - start;
        final long writeQueries = totalWriteQueries.get();
        final long readQueries = totalReadQueries.get();
        return 1000 * (writeQueries + readQueries) / totalRunningTime;
    }

    public void logStats() throws FileNotFoundException {
        printWriter = new PrintWriter("log.txt");
        printWriter.println(new Date() + " Read requests: " + getTotalReadQueries() + " Write requests: " + getTotalWriteQueries() + " Total rate: " + getTotalRPS() + "rps");
        printWriter.flush();
    }
}
