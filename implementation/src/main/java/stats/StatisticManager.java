package stats;

import events.ABCEvent;
import main.Main;
import operator_exploration.GlobalStats;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;

public class StatisticManager {

    private long startTime = -1;
    private long endTime = -1;

    public int numberOfEventsProcessed = 0;
    public int numerOfEventsOOO = 0;
    public HashMap<String, Integer> numberOfOOOPerSource;
    public HashMap<String, Integer> numberOfEventsPerSource;

    public long avgOutOfOrderness;
    public long maxOutOfOrderness;
    public long minOutOfOrderness;

    public HashMap<String, Double> avgOutOfOrdernessPerSource;
    public HashMap<String, Double> maxOutOfOrdernessPerSource;
    public HashMap<String, Double> minOutOfOrdernessPerSource;

    public HashMap<String, Double> avgOOOScorePerSource;

    public HashMap<String, Long> actualArrivalRate;
    public HashMap<String, Long> estimatedArrivalRate;

    private long maxLatency = Long.MIN_VALUE;
    private long minLatency = Long.MAX_VALUE;
    private long avgLatency = 0;

    private int numOfMatches = 0;

    public long slc;

    public double a = 0.6;
    public double b = 0.2;
    public double c = 0.2;
    public double threshold_factor = 2.5;
    Runtime runtime;
    String engine;
    String pattern;
    int windowSize;
    String dataset;

    public StatisticManager(){}

    public StatisticManager(double a, double b, double c, double t){
        this.a = a;
        this.b = b;
        this.c = c;
        this.slc = 0;
        this.threshold_factor = t;
        runtime = Runtime.getRuntime();
    }

    public StatisticManager(double v, double v1, double v2, double v3, String engine,String pattern, int windowSize, String dataset) {
        this(v,v1,v2,v3);
        this.engine = engine;
        this.pattern = pattern;
        this.windowSize = windowSize;
        this.dataset = dataset;
    }

    public void initializeManager(ArrayList<String> sources, HashMap<String, Long> estimatedArrivalRate){
        this.estimatedArrivalRate = estimatedArrivalRate;
        this.initializeManager(sources);
    }

    public void initializeManager(ArrayList<String> sources){
        avgOutOfOrderness = 0;
        maxOutOfOrderness = 0;
        minOutOfOrderness = Long.MAX_VALUE;

        numberOfEventsPerSource = new HashMap<>();
        numberOfOOOPerSource = new HashMap<>();

        avgOOOScorePerSource = new HashMap<>();

        avgOutOfOrdernessPerSource = new HashMap<>();
        maxOutOfOrdernessPerSource = new HashMap<>();
        minOutOfOrdernessPerSource = new HashMap<>();

        if (actualArrivalRate == null)
            actualArrivalRate = new HashMap<>();
        if (estimatedArrivalRate == null)
            estimatedArrivalRate = new HashMap<>();

        for(String source: sources){
            avgOutOfOrdernessPerSource.put(source, 0.0);
            maxOutOfOrdernessPerSource.put(source, 0.0);
            minOutOfOrdernessPerSource.put(source, Double.MAX_VALUE);
            avgOOOScorePerSource.put(source,0.0);
            numberOfEventsPerSource.put(source,0);
            numberOfOOOPerSource.put(source,0);
        }
    }

    public void setParameters(double a1, double b2, double c3){
        a = a1;
        b = b2;
        c = c3;
    }

    //Calculates the out-of-orderness score for a specific event
    public double calculateScore(ABCEvent e, String source, ABCEvent last, Long timeWindow){
        String type = e.getEventType();

        double timeDifference = Math.log(1 + calculateTimeDifference(e, type, last));
        double differenceArrivalRate = Math.pow(calculateDifferenceArrivalRate(type), 2);
        double windowPercent = actualArrivalRate.get(type) / (double) timeWindow;

        double maxWindowPercent = calculateMaxWindowPercent(timeWindow);
        double normalizedWindowPercent = windowPercent / maxWindowPercent;


        double score = a * timeDifference + b * differenceArrivalRate + c * windowPercent;

        return score;
    }

    private double calculateDifferenceArrivalRate(String source) {
        long actualRate = actualArrivalRate.get(source);
        long estimatedRate = estimatedArrivalRate.get(source);
        return Math.abs(actualRate - estimatedRate);
    }

    private double calculateTimeDifference(ABCEvent e, String source, ABCEvent last) {
        return Math.abs(e.getTimestampDate().getTime()- last.getTimestampDate().getTime() - actualArrivalRate.get(source));
    }

    public double calculateThreshold(String source){
        return avgOOOScorePerSource.get(source) * threshold_factor;
    }

    public void setEstimated(HashMap<String, Long> estimated) {
        this.estimatedArrivalRate = estimated;
        this.actualArrivalRate = estimated;
    }

    public void processUpdateStats(ABCEvent e, double score, double timediff, String source, boolean isOOO) {
        String type = e.getEventType();
        if(isOOO){
            if(timediff > maxOutOfOrdernessPerSource.get(type))
                maxOutOfOrdernessPerSource.put(type,timediff);

            if(timediff < minOutOfOrdernessPerSource.get(type))
                minOutOfOrdernessPerSource.put(type,timediff);

            numerOfEventsOOO ++;
            numberOfOOOPerSource.put(type, numberOfOOOPerSource.get(type)+1);

            maxOutOfOrderness = timediff>maxOutOfOrderness? (long) timediff :maxOutOfOrderness;
            minOutOfOrderness = timediff<maxOutOfOrderness? (long) timediff :minOutOfOrderness;
            avgOutOfOrderness = (long) (( (numerOfEventsOOO-1) * avgOutOfOrderness + timediff) / numerOfEventsOOO);

            int num = numberOfOOOPerSource.get(type);
            double newavg = ( (num-1) * avgOOOScorePerSource.get(type) + score) / num;
            avgOOOScorePerSource.put(type,newavg);
        }

        int numT = numberOfEventsPerSource.get(type);
        double newavgT = ( (numT-1) * avgOutOfOrdernessPerSource.get(type) + timediff) / numT;
        avgOutOfOrdernessPerSource.put(type,newavgT);

    }

    private void adaptSlack(long time_window){
        double percentage = (double) numerOfEventsOOO/numberOfEventsProcessed;

        percentage = 0;
        if(percentage <= 0.1)
            slc = 0;
        else {
            long maxSlack = (long) (percentage * time_window);
            slc = Math.min(maxOutOfOrderness, maxSlack)/10;
        }
    }

    public long getSlc(long tw) {
        adaptSlack(tw);
        return slc;
    }

    public void updateStats(ABCEvent e){
        if (startTime == -1) {
            startTime = System.currentTimeMillis();
        }
        endTime = System.currentTimeMillis();
        String type = e.getEventType();
        numberOfEventsPerSource.put(type, numberOfEventsPerSource.get(type) + 1);
        numberOfEventsProcessed ++;
    }

    public void updateLatencyProfiling(long latency) {
        numOfMatches++;

        // Update latency stats
        maxLatency = Math.max(latency, maxLatency);
        minLatency = Math.min(latency, minLatency);
        avgLatency = ((numOfMatches - 1) * avgLatency + latency) / numOfMatches;
    }

    private double calculateMaxWindowPercent(Long timeWindow) {
        return estimatedArrivalRate.values()
                .stream()
                .mapToDouble(rate -> rate / (double) timeWindow)
                .max()
                .orElse(1.0);
    }

//    public void printProfiling(){
//        System.out.println("===== STATISTIC PROFILING REPORT =====");
//        System.out.println("Global Event Stats:");
//        System.out.println(" - Total Events Processed: " + numberOfEventsProcessed);
//        System.out.println(" - Total Out-Of-Order Events: " + numerOfEventsOOO);
//        System.out.println(" - Memory Used: " + (runtime.totalMemory() - runtime.freeMemory())/(1024L*1024L));
//        System.out.println(" - Avg Out-Of-Orderness: " + avgOutOfOrderness);
//        System.out.println(" - Max Out-Of-Orderness: " + maxOutOfOrderness);
//        System.out.println(" - Min Out-Of-Orderness: " + (minOutOfOrderness == Long.MAX_VALUE ? "N/A" : minOutOfOrderness));
//        System.out.println(" - Slack (SLC): " + slc);
//
//        if (startTime > 0 && endTime > startTime) {
//            long durationMillis = endTime - startTime;
//            double durationSeconds = durationMillis / 1000.0;
//            double throughput = numberOfEventsProcessed / durationSeconds;
//
//            System.out.println(" - Total Processing Time: " + durationMillis + " ms (" + durationSeconds + " s)");
//            System.out.printf(" - Throughput: %.2f events/second%n", throughput);
//        } else {
//            System.out.println(" - Total Processing Time: N/A");
//            System.out.println(" - Throughput: N/A (not enough timing data)");
//        }
//
//        System.out.println("\nPer Source Statistics:");
//        for (String source : numberOfEventsPerSource.keySet()) {
//            System.out.println(" -> Source: " + source);
//            System.out.println("    - Events Processed: " + numberOfEventsPerSource.get(source));
//            System.out.println("    - OOO Events: " + numberOfOOOPerSource.get(source));
//            System.out.println("    - Avg OOO Score: " + avgOOOScorePerSource.get(source));
//            System.out.println("    - Avg Out-Of-Orderness: " + avgOutOfOrdernessPerSource.get(source));
//            System.out.println("    - Max Out-Of-Orderness: " + maxOutOfOrdernessPerSource.get(source));
//            System.out.println("    - Min Out-Of-Orderness: " +
//                    (minOutOfOrdernessPerSource.get(source) == Double.MAX_VALUE ? "N/A" : minOutOfOrdernessPerSource.get(source)));
//            System.out.println("    - Estimated Arrival Rate: " + estimatedArrivalRate.getOrDefault(source, -1L));
//            System.out.println("    - Actual Arrival Rate: " + actualArrivalRate.getOrDefault(source, -1L));
//        }
//
//        System.out.println("\nLatency Profiling:");
//        System.out.println(" - Matches Count: " + numOfMatches);
//        System.out.println(" - Max Latency (ns): " + (maxLatency == Long.MIN_VALUE ? "N/A" : maxLatency));
//        System.out.println(" - Min Latency (ns): " + (minLatency == Long.MAX_VALUE ? "N/A" : minLatency));
//        System.out.println(" - Avg Latency (ns): " + (numOfMatches > 0 ? avgLatency : "N/A"));
//
//        System.out.println("\nParameters Used:");
//        System.out.println(" - a: " + a + "  b: " + b + "  c: " + c);
//        System.out.println(" - threshold_factor: " + threshold_factor);
//
//        System.out.println("\nOperator Runtime Stats:");
//        GlobalStats.printOperatorStats();
//
//        System.out.println("======================================");
//    }


    public void printProfiling() {

        Path outputDir = Paths.get("src/main/resources/new_experiments/sens_an/");
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String baseName = String.format(
                "w%d_%s_%s_%s_%s",
                windowSize,
                numberOfEventsProcessed,
                engine,
                pattern,
                dataset
        );

        Path filepath = getAvailableFile(outputDir, baseName);

            try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(filepath))) {

                out.println("===== STATISTIC PROFILING REPORT =====");
                out.println("Global Event Stats:");
                out.println(" - Total Events Processed: " + numberOfEventsProcessed);
                out.println(" - Total Out-Of-Order Events: " + numerOfEventsOOO);
                out.println(" - Memory Used: " + (runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L));
                out.println(" - Avg Out-Of-Orderness: " + avgOutOfOrderness);
                out.println(" - Max Out-Of-Orderness: " + maxOutOfOrderness);
                out.println(" - Min Out-Of-Orderness: " +
                        (minOutOfOrderness == Long.MAX_VALUE ? "N/A" : minOutOfOrderness));
                out.println(" - Slack (SLC): " + slc);

                if (startTime > 0 && endTime > startTime) {
                    long durationMillis = endTime - startTime;
                    double durationSeconds = durationMillis / 1000.0;
                    double throughput = numberOfEventsProcessed / durationSeconds;

                    out.println(" - Total Processing Time: " + durationMillis + " ms (" + durationSeconds + " s)");
                    out.printf(" - Throughput: %.2f events/second%n", throughput);
                } else {
                    out.println(" - Total Processing Time: N/A");
                    out.println(" - Throughput: N/A (not enough timing data)");
                }

                out.println("\nPer Source Statistics:");
                for (String source : numberOfEventsPerSource.keySet()) {
                    out.println(" -> Source: " + source);
                    out.println("    - Events Processed: " + numberOfEventsPerSource.get(source));
                    out.println("    - OOO Events: " + numberOfOOOPerSource.get(source));
                    out.println("    - Avg OOO Score: " + avgOOOScorePerSource.get(source));
                    out.println("    - Avg Out-Of-Orderness: " + avgOutOfOrdernessPerSource.get(source));
                    out.println("    - Max Out-Of-Orderness: " + maxOutOfOrdernessPerSource.get(source));
                    out.println("    - Min Out-Of-Orderness: " +
                            (minOutOfOrdernessPerSource.get(source) == Double.MAX_VALUE
                                    ? "N/A"
                                    : minOutOfOrdernessPerSource.get(source)));
                    out.println("    - Estimated Arrival Rate: " + estimatedArrivalRate.getOrDefault(source, -1L));
                    out.println("    - Actual Arrival Rate: " + actualArrivalRate.getOrDefault(source, -1L));
                }

                out.println("\nLatency Profiling:");
                out.println(" - Matches Count: " + numOfMatches);
                out.println(" - Max Latency (ns): " + (maxLatency == Long.MIN_VALUE ? "N/A" : maxLatency));
                out.println(" - Min Latency (ns): " + (minLatency == Long.MAX_VALUE ? "N/A" : minLatency));
                out.println(" - Avg Latency (ns): " + (numOfMatches > 0 ? avgLatency : "N/A"));

                out.println("\nParameters Used:");
                out.println(" - a: " + a + "  b: " + b + "  c: " + c);
                out.println(" - threshold_factor: " + threshold_factor);

                out.println("\nOperator Runtime Stats:");
                GlobalStats.printOperatorStats(out); // better if you can change this method too

                out.println("======================================");

                Main.trieManager.printPTstats(out);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

        System.out.println("Profiling report written to: " + filepath.toAbsolutePath());
    }

    public static Path getAvailableFile(Path directory, String baseName) {
        Path file = directory.resolve(baseName + ".txt");

        if (!Files.exists(file)) {
            return file;
        }

        int run = 1;
        while (true) {
            file = directory.resolve(baseName + "_" + run + ".txt");
            if (!Files.exists(file)) {
                return file;
            }
            run++;
        }
    }
}
