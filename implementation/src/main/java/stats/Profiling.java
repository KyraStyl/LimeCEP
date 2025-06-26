package stats;

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

        System.out.println();
        System.out.println("**************Profiling Numbers*****************");
        System.out.println("Solution: "+ this.solution);
        if(this.alpha > -1)
            System.out.println("Alpha adaptation: "+ this.alpha);
        System.out.println("Number Of Events Processed: " + numberOfEvents);
        System.out.println("Number Of Matches Found: " + numOfMatches);
        System.out.println("Used memory is bytes: " + memoryUsed);
        System.out.println("Used memory is megabytes: " + memoryUsed/(1024L*1024L));

        System.out.println("Maximum Latency in nano: " + maxLatency);
        System.out.println("Minimum Latency in nano: " + minLatency);



        if (numOfMatches > 0)
            System.out.println("Average Latency in nano: " + avgLatency);
        else
            System.out.println("No matches found!");

    }

    public void increaseEvents() {
        numberOfEvents++;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }
}
