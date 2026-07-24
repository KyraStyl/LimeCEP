package stats;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static stats.StatisticManager.getAvailableFile;

public class Profiling {

    private long maxLatency;
    private long minLatency;
    private long avgLatency;

    private int numOfMatches;

    private long memoryUsed;
    private long maxMemoryUsed;

    private int numberOfEvents;
    private String solution;
    private double alpha;

    public Profiling(String solution){
        maxLatency = Long.MIN_VALUE;
        minLatency = Long.MAX_VALUE;
        avgLatency = Long.MIN_VALUE;

        numOfMatches = 0;

        memoryUsed = 0;
        maxMemoryUsed = 0;

        numberOfEvents = 0;
        this.solution = solution;
        this.alpha = -1;
    }

    private void updateMaxLatency(long latency){
        maxLatency = latency > maxLatency ? latency : maxLatency;
    }

    private void updateMinLatency(long latency){
        minLatency = latency < minLatency ? latency : minLatency;
    }

    private void updateAvgLatency(long latency){
        avgLatency = ((numOfMatches-1) * avgLatency + latency) / numOfMatches;
    }

    private void updateLatency(long l){
        updateMaxLatency(l);
        updateMinLatency(l);
        updateAvgLatency(l);
    }

    public void updateProfiling(long latency){
        numOfMatches ++;
        memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        maxMemoryUsed = memoryUsed > maxMemoryUsed ? memoryUsed : maxMemoryUsed;
        updateLatency(latency);
    }

    public void printProfiling(){
        Path outputDir = Paths.get("src/main/resources/new_experiments/sens_an/");
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String baseName = String.format(
                "w%d_%s_%s_%s",
                100,
                numberOfEvents,
                "sase",
                "abc"
        );

        Path filepath = getAvailableFile(outputDir, baseName);

        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(filepath))) {
            out.println();
            out.println("**************Profiling Numbers*****************");
            out.println("Solution: " + this.solution);
            if (this.alpha > -1)
                out.println("Alpha adaptation: " + this.alpha);
            out.println("Number Of Events Processed: " + numberOfEvents);
            out.println("Number Of Matches Found: " + numOfMatches);
            out.println("Used memory is bytes: " + memoryUsed);
            out.println("Used memory is megabytes: " + memoryUsed / (1024L * 1024L));

            out.println("Maximum Latency in nano: " + maxLatency);
            out.println("Minimum Latency in nano: " + minLatency);


            if (numOfMatches > 0)
                out.println("Average Latency in nano: " + avgLatency);
            else
                out.println("No matches found!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("Profiling report written to: " + filepath.toAbsolutePath());

    }

    public void increaseEvents() {
        numberOfEvents++;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }
}
