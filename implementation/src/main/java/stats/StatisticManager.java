package stats;

import events.ABCEvent;

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

    public StatisticManager(){}

    public StatisticManager(double a, double b, double c, double t){
        this.a = a;
        this.b = b;
        this.c = c;
        this.slc = 0;
        this.threshold_factor = t;
        runtime = Runtime.getRuntime();
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

    public void printProfiling(){
        System.out.println("===== STATISTIC PROFILING REPORT =====");
        System.out.println("Global Event Stats:");
        System.out.println(" - Total Events Processed: " + numberOfEventsProcessed);
        System.out.println(" - Total Out-Of-Order Events: " + numerOfEventsOOO);
        System.out.println(" - Memory Used: " + (runtime.totalMemory() - runtime.freeMemory())/(1024L*1024L));
        System.out.println(" - Avg Out-Of-Orderness: " + avgOutOfOrderness);
        System.out.println(" - Max Out-Of-Orderness: " + maxOutOfOrderness);
        System.out.println(" - Min Out-Of-Orderness: " + (minOutOfOrderness == Long.MAX_VALUE ? "N/A" : minOutOfOrderness));
        System.out.println(" - Slack (SLC): " + slc);

        if (startTime > 0 && endTime > startTime) {
            long durationMillis = endTime - startTime;
            double durationSeconds = durationMillis / 1000.0;
            double throughput = numberOfEventsProcessed / durationSeconds;

            System.out.println(" - Total Processing Time: " + durationMillis + " ms (" + durationSeconds + " s)");
            System.out.printf(" - Throughput: %.2f events/second%n", throughput);
        } else {
            System.out.println(" - Total Processing Time: N/A");
            System.out.println(" - Throughput: N/A (not enough timing data)");
        }

        System.out.println("\nPer Source Statistics:");
        for (String source : numberOfEventsPerSource.keySet()) {
            System.out.println(" -> Source: " + source);
            System.out.println("    - Events Processed: " + numberOfEventsPerSource.get(source));
            System.out.println("    - OOO Events: " + numberOfOOOPerSource.get(source));
            System.out.println("    - Avg OOO Score: " + avgOOOScorePerSource.get(source));
            System.out.println("    - Avg Out-Of-Orderness: " + avgOutOfOrdernessPerSource.get(source));
            System.out.println("    - Max Out-Of-Orderness: " + maxOutOfOrdernessPerSource.get(source));
            System.out.println("    - Min Out-Of-Orderness: " +
                    (minOutOfOrdernessPerSource.get(source) == Double.MAX_VALUE ? "N/A" : minOutOfOrdernessPerSource.get(source)));
            System.out.println("    - Estimated Arrival Rate: " + estimatedArrivalRate.getOrDefault(source, -1L));
            System.out.println("    - Actual Arrival Rate: " + actualArrivalRate.getOrDefault(source, -1L));
        }

        System.out.println("\nLatency Profiling:");
        System.out.println(" - Matches Count: " + numOfMatches);
        System.out.println(" - Max Latency (ns): " + (maxLatency == Long.MIN_VALUE ? "N/A" : maxLatency));
        System.out.println(" - Min Latency (ns): " + (minLatency == Long.MAX_VALUE ? "N/A" : minLatency));
        System.out.println(" - Avg Latency (ns): " + (numOfMatches > 0 ? avgLatency : "N/A"));

        System.out.println("\nParameters Used:");
        System.out.println(" - a: " + a + "  b: " + b + "  c: " + c);
        System.out.println(" - threshold_factor: " + threshold_factor);

        System.out.println("======================================");
    }
}
