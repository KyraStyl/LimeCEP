package operator_exploration;

import semi_automated.PatternTrie;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class GlobalStats {

    // Operator timing stats
    public static long extensionTime = 0;
    public static long variationTime = 0;
    public static long swapTime = 0;

    public static long extensionCalls = 0;
    public static long variationCalls = 0;
    public static long swapCalls = 0;

    public static long generatedCandidates = 0;
    public static long activatedCandidates = 0;
    public static long generatedPatternsWithExtension = 0;
    public static long generatedPatternsWithVariation = 0;
    public static long generatedPatternsWithSwap = 0;
    public static long prunedCandidates = 0;

    public double minP(String pattern) {
        double product = 1.0;
        for (char c : pattern.toCharArray()) {
            product *= getProbability(String.valueOf(c));
        }
        return product;
    }

    public double maxP(String pattern) {
        double min = 1.0;
        for (char c : pattern.toCharArray()) {
            min = Math.min(min, getProbability(String.valueOf(c)));
        }
        return min == Double.MAX_VALUE ? 0 : min;
    }

    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final Map<String, Map<String, AtomicLong>> transitions = new ConcurrentHashMap<>();
    private final AtomicLong total = new AtomicLong(0);

    private String lastSeenType = null;

    // =========================================================
    // Update statistics per event
    // =========================================================

    public void update(String eventType) {

        counts.computeIfAbsent(eventType, k -> new AtomicLong(0)).incrementAndGet();
        total.incrementAndGet();

        if (lastSeenType != null) {
            transitions
                    .computeIfAbsent(lastSeenType, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(eventType, k -> new AtomicLong(0))
                    .incrementAndGet();
        }

        lastSeenType = eventType;
    }

    // =========================================================
    // Marginal probability
    // =========================================================

    public double getProbability(String type) {

        long t = total.get();
        if (t == 0) return 0.0;

        AtomicLong c = counts.get(type);
        if (c == null) return 0.0;

        return (double) c.get() / t;
    }

    // =========================================================
    // Conditional probability P(Y|X)
    // =========================================================

    public double conditionalProb(String prev, String curr) {

        Map<String, AtomicLong> map = transitions.get(prev);
        if (map == null) return 0.0;

        AtomicLong count = map.get(curr);
        if (count == null) return 0.0;

        AtomicLong prevCount = counts.get(prev);
        if (prevCount == null || prevCount.get() == 0) return 0.0;

        return (double) count.get() / prevCount.get();
    }

    // =========================================================
    // AQP Greedy Estimate
    // =========================================================

    public double greedyEstimate(String pattern) {

        if (pattern.length() == 0)
            return 0.0;

        double prob = getProbability(String.valueOf(pattern.charAt(0)));

        for (int i = 1; i < pattern.length(); i++) {

            prob *= conditionalProb(
                    String.valueOf(pattern.charAt(i - 1)),
                    String.valueOf(pattern.charAt(i)));
        }

        return prob;
    }

    // ========================================================
    // Update counters
    // ========================================================

    public void updateExtensionStats(int number, int activated, int pruned) {
        extensionCalls+=number;
        generatedCandidates+=number;
        generatedPatternsWithExtension+=activated;
        prunedCandidates+=pruned;
    }

    public void updateVariationStats(int number, int activated, int pruned) {
        variationCalls+=number;
        generatedCandidates+=number;
        generatedPatternsWithVariation+=activated;
        prunedCandidates+=pruned;
    }

    public void updateSwapStats(int number, int activated, int pruned) {
        swapCalls+=number;
        generatedCandidates+=number;
        generatedPatternsWithSwap+=activated;
        prunedCandidates+=pruned;
    }

    public void updateActivatedNumbers(int num){
        activatedCandidates = num;
    }

    // =========================================================
    // Access helpers
    // =========================================================

    public Set<String> getAllTypes() {
        return counts.keySet();
    }

    public static void printOperatorStats(PrintWriter out) {

        out.println("===== Operator Runtime Stats =====");

        if (extensionCalls > 0)
            out.println("Extension avg time: "
                    + (extensionTime / extensionCalls) / 1_000_000.0 + " ms");

        if (variationCalls > 0)
            out.println("Variation avg time: "
                    + (variationTime / variationCalls) / 1_000_000.0 + " ms");

        if (swapCalls > 0)
            out.println("Swap avg time: "
                    + (swapTime / swapCalls) / 1_000_000.0 + " ms");


        out.println("Total Generated candidates before deduplication: " + generatedCandidates);
        out.println("Activated candidates: " + activatedCandidates);
        out.println("Generated patterns with extension: " + generatedPatternsWithExtension);
        out.println("Generated patterns with variation: " + generatedPatternsWithVariation);
        out.println("Generated patterns with swap: " + generatedPatternsWithSwap);
        out.println("Total generated patterns: " + (generatedPatternsWithExtension+generatedPatternsWithVariation+generatedPatternsWithSwap));
        out.println("Pruned candidates: " + prunedCandidates);

        out.println("==================================");
    }

    public Map<String, Map<String, AtomicLong>> getTransitions() {
        return transitions;
    }
}
