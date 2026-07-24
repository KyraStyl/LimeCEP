package operator_exploration;


import main.Main;
import semi_automated.PatternTrie;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class OperatorExplorer {

    private final PatternTrie trie;
    private final GlobalStats stats;

    private final double thetaFreq;
    private final double thetaRare;

    private final int minLength;
    private final int maxPatternLength;
    private final int budget;
    private final double gainThreshold = 0.3;
    private final int k;

    public OperatorExplorer(PatternTrie trie,
                            GlobalStats stats,
                            double thetaFreq,
                            double thetaRare,
                            int minLength,
                            int maxPatternLength,
                            int budget) {

        this.trie = trie;
        this.stats = stats;
        this.thetaFreq = thetaFreq;
        this.thetaRare = thetaRare;
        this.minLength = minLength;
        this.maxPatternLength = maxPatternLength;
        this.budget = budget;
        this.k = 20;
    }

    public Set<String> runBatch() {

        if(Main.debug)
            System.out.println("RUNNING BATCH OPERATOR EXPLORATION");

        Set<String> active = trie.getActivatedPatterns();
        if(Main.debug)
            for(String q : active) {
                System.out.println("Active pattern: " + q);
            }

        Map<String, Map<String, AtomicLong>> transitions = stats.getTransitions();
        if(Main.debug)
            for(String t: transitions.keySet()) {
                System.out.println("Transition: " + t + "   " + transitions.get(t)+" with count = " + transitions.get(t).size() + "");
            }

        Set<String> sigma = getEligibleTypes();

        Set<String> candidates = new HashSet<>();

        for (String q : active) {

            if (!isExpandable(q))
                continue;

            long startExt = System.nanoTime();
            candidates.addAll(applyExtension(q, sigma));
            stats.extensionTime += System.nanoTime() - startExt;
            stats.extensionCalls++;

            long startVar = System.nanoTime();
            candidates.addAll(applyVariation(q, sigma));
            stats.variationTime += System.nanoTime() - startVar;
            stats.variationCalls++;

            long startSwap = System.nanoTime();
            candidates.addAll(applySwap(q));
            stats.swapTime += System.nanoTime() - startSwap;
            stats.swapCalls++;
        }

        activateTopCandidates(candidates);
        stats.updateActivatedNumbers(trie.countActivatedPatterns());
        if(Main.debug)
            System.out.println("END OF BATCH OPERATOR EXPLORATION");
        return candidates;
    }

    // =========================================================

    private boolean isExpandable(String q) {

        double maxP = stats.maxP(q);
        double minP = computeMinP(q);

        return (q.length()<maxPatternLength && (maxP >= thetaFreq || minP < thetaRare));
    }

    private boolean isActivatable(String q) {

        if (q.length() < minLength) return false;
        if (q.length() > maxPatternLength) return false;

        double maxP = stats.maxP(q);
        double minP = computeMinP(q);

        if(Main.debug)
            if(minP < thetaRare)
                System.out.println("RARE pattern "+q);
            else if(maxP >= thetaFreq)
                System.out.println("FREQUENT pattern "+q);

        return (maxP >= thetaFreq || minP < thetaRare);
    }

    private Set<String> getEligibleTypes() {

        Set<String> result = new HashSet<>();

        for (String t : stats.getAllTypes()) {

            double p = stats.getProbability(t);

            if (p >= thetaFreq || p <= thetaRare)
                result.add(t);
        }

        return result;
    }

    private Set<String> applyExtension(String q, Set<String> sigma) {

        int pruned = 0;
        int activated = 0;
        Set<String> out = new HashSet<>();
        String last = String.valueOf(q.charAt(q.length() - 1));
        Set<String> localSigma = new HashSet<>(sigma);
        localSigma.remove(last);

        for (String T : localSigma) {
                String candidate = q + T;
                if(Main.debug)
                    System.out.println("Exploring EXTENSION Candidate: " + candidate);

                if (isActivatable(candidate) && worthsActivating(q, candidate)) {
                    out.add(candidate);
                    activated++;
                }else
                    pruned++;
        }

        stats.updateExtensionStats(localSigma.size(),activated,pruned);

        return out;
    }

    private Set<String> applyVariation(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();

        int bottleneckMin = findBottleneckMin(q);
        int bottleneckMax = findBottleneckMax(q);

        Set<String> localSigma = new HashSet<>(sigma);

        localSigma.remove(String.valueOf(q.charAt(bottleneckMin)));
        out.addAll(findVariationCandidates(q, localSigma, bottleneckMin));

        localSigma = new HashSet<>(sigma);

        localSigma.remove(String.valueOf(q.charAt(bottleneckMax)));
        out.addAll(findVariationCandidates(q, localSigma, bottleneckMax));

        return out;
    }

    private Set<String> findVariationCandidates(String q, Set<String> sigma, int bottleneck) {
        int pruned = 0;
        int generated = 0;
        int activated = 0;
        Set<String> out = new HashSet<>();
        for (String T : sigma) {

            if (!String.valueOf(q.charAt(bottleneck)).equals(T) && (bottleneck>0 && !String.valueOf(q.charAt(bottleneck - 1)).equals(T))) {
                generated++;
                String candidate =
                        q.substring(0, bottleneck)
                                + T
                                + q.substring(bottleneck + 1);
                if(Main.debug)
                    System.out.println("Exploring VARIATION Candidate: " + candidate);

                if (isActivatable(candidate) && worthsActivating(q, candidate)) {
                    out.add(candidate);
                    activated++;
                }else
                    pruned++;

            }
        }
        stats.updateVariationStats(generated,activated,pruned);
        return out;
    }

    private Set<String> applySwap(String q) {

        Set<String> out = new HashSet<>();

        out.addAll(findSwapCandidates(q, findBottleneckMin(q)));
        out.addAll(findSwapCandidates(q, findBottleneckMax(q)));

        return out;
    }

    private Set<String> findSwapCandidates(String q, int bottleneck) {
        int generated = 0;
        int pruned = 0;
        int activated = 0;

        Set<String> out = new HashSet<>();
        if (bottleneck > 0) {
            if(!(bottleneck>1 && String.valueOf(q.charAt(bottleneck - 2)).equals(String.valueOf(q.charAt(bottleneck))))) {
                String c = swap(q, bottleneck - 1);
                generated++;
                if(Main.debug)
                    System.out.println("Exploring SWAP Candidate: " + c);
                if (isActivatable(c) && worthsActivating(q, c)) {
                    out.add(c);
                    activated++;
                }else
                    pruned++;
            }
        }

        if (bottleneck < q.length() - 1) {
            if(!(bottleneck>1 && String.valueOf(q.charAt(bottleneck - 1)).equals(String.valueOf(q.charAt(bottleneck+1))))) {
                String c = swap(q, bottleneck);
                generated++;
                if(Main.debug)
                    System.out.println("Exploring SWAP Candidate: " + c);
                if (isActivatable(c) && worthsActivating(q, c)) {
                    out.add(c);
                    activated++;
                }else
                    pruned++;
            }
        }

        stats.updateSwapStats(generated,activated,pruned);

        return out;
    }

    private void activateTopCandidates(Set<String> candidates) {

        List<String> sorted = new ArrayList<>(candidates);

        sorted.sort((a, b) ->
                Double.compare(
                        comparableAQPscore(b),
                        comparableAQPscore(a)));

        if(Main.debug)
            for(int i=0;i<sorted.size() && i<budget;i++)
                System.out.println("Candidate: " + sorted.get(i) + "   greedy-Conditional-prob = " + stats.greedyEstimate(sorted.get(i)));

        String c;
        int count = trie.getActivatedPatterns().size();
        for (int i=0;i<sorted.size() && i<k && count+k<=budget;i++) {
            c = sorted.get(i);
            if (!trie.exists(c) && isActivatable(c)) {
                if(Main.debug) {
                    System.out.println("Candidate: " + c + "   greedy-Conditional-prob = " + stats.greedyEstimate(c));
                    System.out.println("Candidate: " + c + "   MaxP(q) = " + stats.maxP(c));
                    System.out.println("Candidate: " + c + "   MinP(q) = " + stats.minP(c));
                    System.out.println("Activated pattern: " + c);
                }
                trie.insert(c, false);
                Main.trieManager.activatePattern(c);
//                trie.calculateConfidence(c);
            }
        }
    }

    private boolean worthsActivating(String q, String c) {

        double parentP = stats.greedyEstimate(q);
        double childP = stats.greedyEstimate(c);

        double gain = childP / parentP;

        if(gain >= this.gainThreshold)
            return true;
        return false;
    }

    private int findBottleneckMin(String q) {

        double min = 1.0;
        int index = 0;

        for (int i = 0; i < q.length(); i++) {

            double p = stats.getProbability(
                    String.valueOf(q.charAt(i)));

            if (p < min) {
                min = p;
                index = i;
            }
        }

        return index;
    }

    private int findBottleneckMax(String q) {

        double max = 0.0;
        int index = 0;

        for (int i = 0; i < q.length(); i++) {

            double p = stats.getProbability(
                    String.valueOf(q.charAt(i)));

            if (p > max) {
                max = p;
                index = i;
            }
        }

        return index;
    }

    private double computeMinP(String pattern) {

        double product = 1.0;

        for (char c : pattern.toCharArray())
            product *= stats.getProbability(
                    String.valueOf(c));

        return product;
    }

    private String swap(String q, int i) {

        char[] arr = q.toCharArray();
        char tmp = arr[i];
        arr[i] = arr[i + 1];
        arr[i + 1] = tmp;

        return new String(arr);
    }

    private double comparableAQPscore(String q) {
        double p = stats.greedyEstimate(q);

        if (p == 0)
            return Double.MAX_VALUE;

        double freqScore = p / thetaFreq;
        double rareScore = thetaRare / p;

        return Math.max(freqScore, rareScore);
    }

    // ============================================================
    // ============================================================
    // FOR EXPERIMENTS

    public Set<String> runBatchAblation(
            boolean useEligibleTypes,
            boolean useImportanceFilter,
            boolean useBottleneckPruning,
            boolean useExtension,
            boolean useVariation,
            boolean useSwap
    ) {
        if (Main.debug)
            System.out.println("RUNNING BATCH ABLATION OPERATOR EXPLORATION");

        Set<String> active = trie.getActivatedPatterns();

        Set<String> sigma = useEligibleTypes
                ? getEligibleTypes()
                : stats.getAllTypes();

        Set<String> candidates = new HashSet<>();

        for (String q : active) {

            if (q.length() >= maxPatternLength)
                continue;

            if (useImportanceFilter && !isExpandable(q))
                continue;

            if (useExtension) {
                long startExt = System.nanoTime();

                candidates.addAll(
                        useImportanceFilter
                                ? applyExtension(q, sigma)
                                : applyExtensionNoFilter(q, sigma)
                );

                stats.extensionTime += System.nanoTime() - startExt;
                stats.extensionCalls++;
            }

            if (useVariation) {
                long startVar = System.nanoTime();

                candidates.addAll(
                        useBottleneckPruning
                                ? (useImportanceFilter
                                ? applyVariation(q, sigma)
                                : applyVariationNoFilter(q, sigma))
                                : applyExhaustiveVariation(q, sigma, useImportanceFilter)
                );

                stats.variationTime += System.nanoTime() - startVar;
                stats.variationCalls++;
            }

            if (useSwap) {
                long startSwap = System.nanoTime();

                candidates.addAll(
                        useBottleneckPruning
                                ? (useImportanceFilter
                                ? applySwap(q)
                                : applySwapNoFilter(q))
                                : applyExhaustiveSwap(q, useImportanceFilter)
                );

                stats.swapTime += System.nanoTime() - startSwap;
                stats.swapCalls++;
            }
        }

        activateTopCandidates(candidates);

        if (Main.debug)
            System.out.println("END OF BATCH ABLATION OPERATOR EXPLORATION");

        return candidates;
    }

    private Set<String> applyExtensionNoFilter(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();
        String last = String.valueOf(q.charAt(q.length() - 1));

        for (String T : sigma) {

            if (last.equals(T))
                continue;

            String candidate = q + T;

            if (candidate.length() <= maxPatternLength)
                out.add(candidate);
        }

        return out;
    }

    private Set<String> applyVariationNoFilter(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();

        int bottleneckMin = findBottleneckMin(q);
        int bottleneckMax = findBottleneckMax(q);

        out.addAll(findVariationCandidatesNoFilter(q, sigma, bottleneckMin));
        out.addAll(findVariationCandidatesNoFilter(q, sigma, bottleneckMax));

        return out;
    }

    private Set<String> findVariationCandidatesNoFilter(String q, Set<String> sigma, int position) {

        Set<String> out = new HashSet<>();

        for (String T : sigma) {

            if (String.valueOf(q.charAt(position)).equals(T))
                continue;

            String candidate =
                    q.substring(0, position)
                            + T
                            + q.substring(position + 1);

            if (candidate.length() <= maxPatternLength)
                out.add(candidate);
        }

        return out;
    }

    private Set<String> applySwapNoFilter(String q) {

        Set<String> out = new HashSet<>();

        out.addAll(findSwapCandidatesNoFilter(q, findBottleneckMin(q)));
        out.addAll(findSwapCandidatesNoFilter(q, findBottleneckMax(q)));

        return out;
    }

    private Set<String> findSwapCandidatesNoFilter(String q, int bottleneck) {

        Set<String> out = new HashSet<>();

        if (bottleneck > 0)
            out.add(swap(q, bottleneck - 1));

        if (bottleneck < q.length() - 1)
            out.add(swap(q, bottleneck));

        out.remove(q);
        return out;
    }

    private Set<String> applyExhaustiveVariation(
            String q,
            Set<String> sigma,
            boolean useImportanceFilter
    ) {
        Set<String> out = new HashSet<>();

        for (int i = 0; i < q.length(); i++) {

            for (String T : sigma) {

                if (String.valueOf(q.charAt(i)).equals(T))
                    continue;

                String candidate =
                        q.substring(0, i)
                                + T
                                + q.substring(i + 1);

                if (candidate.length() > maxPatternLength)
                    continue;

                if (!useImportanceFilter || isActivatable(candidate))
                    out.add(candidate);
            }
        }

        return out;
    }

    private Set<String> applyExhaustiveSwap(
            String q,
            boolean useImportanceFilter
    ) {
        Set<String> out = new HashSet<>();

        for (int i = 0; i < q.length() - 1; i++) {

            String candidate = swap(q, i);

            if (candidate.equals(q))
                continue;

            if (!useImportanceFilter || isActivatable(candidate))
                out.add(candidate);
        }

        return out;
    }

    // ======================================================
    // ======================================================
    // apriori like

    public Set<String> runAprioriBatch() {

        if (Main.debug)
            System.out.println("RUNNING BATCH APRIORI-STYLE EXHAUSTIVE EXPLORATION");

        Set<String> active = trie.getActivatedPatterns();

        // Exhaustive baseline: use all observed event types, not only eligible ones.
        Set<String> sigma = stats.getAllTypes();

        Set<String> candidates = new HashSet<>();

        for (String q : active) {

            if (q.length() >= maxPatternLength)
                continue;

            candidates.addAll(applyExhaustiveExtension(q, sigma));
            candidates.addAll(applyExhaustiveVariation(q, sigma));
            candidates.addAll(applyExhaustiveSwap(q));
        }

        activateTopCandidates(candidates);

        if (Main.debug)
            System.out.println("END OF BATCH APRIORI-STYLE EXHAUSTIVE EXPLORATION");

        return candidates;
    }

    private Set<String> applyExhaustiveExtension(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();

        for (String T : sigma) {

            String candidate = q + T;

            if (Main.debug)
                System.out.println("Exploring APRIORI EXTENSION Candidate: " + candidate);

            if (isActivatable(candidate))
                out.add(candidate);
        }

        return out;
    }

    private Set<String> applyExhaustiveVariation(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();

        for (int i = 0; i < q.length(); i++) {

            String current = String.valueOf(q.charAt(i));

            for (String T : sigma) {

                if (current.equals(T))
                    continue;

                String candidate =
                        q.substring(0, i)
                                + T
                                + q.substring(i + 1);

                if (Main.debug)
                    System.out.println("Exploring APRIORI VARIATION Candidate: " + candidate);

                if (isActivatable(candidate))
                    out.add(candidate);
            }
        }

        return out;
    }

    private Set<String> applyExhaustiveSwap(String q) {

        Set<String> out = new HashSet<>();

        for (int i = 0; i < q.length() - 1; i++) {

            String candidate = swap(q, i);

            if (candidate.equals(q))
                continue;

            if (Main.debug)
                System.out.println("Exploring APRIORI SWAP Candidate: " + candidate);

            if (isActivatable(candidate))
                out.add(candidate);
        }

        return out;
    }

    // ================================================================
    // ================================================================
    // new apriori

    public Set<String> runExhaustiveBatch() {

        if (Main.debug)
            System.out.println("RUNNING EXHAUSTIVE APRIORI-STYLE EXPLORATION");

        Set<String> active = new HashSet<>(trie.getActivatedPatterns());
        Set<String> sigma = stats.getAllTypes();

        Set<String> candidates = new HashSet<>();

        for (String q : active) {

            if (q.length() >= maxPatternLength)
                continue;

            candidates.addAll(applyExhaustiveExtensionAll(q, sigma));
            candidates.addAll(applyExhaustiveVariationAll(q, sigma));
            candidates.addAll(applyExhaustiveSwapAll(q));
        }

        activateAllExhaustiveCandidates(candidates);

        if (Main.debug)
            System.out.println("END OF EXHAUSTIVE APRIORI-STYLE EXPLORATION");

        return candidates;
    }

    private Set<String> applyExhaustiveExtensionAll(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();
        String last = String.valueOf(q.charAt(q.length() - 1));

        for (String T : sigma) {

            if (last.equals(T))
                continue;

            String candidate = q + T;

            if (candidate.length() <= maxPatternLength)
                out.add(candidate);
        }

        return out;
    }

    private Set<String> applyExhaustiveVariationAll(String q, Set<String> sigma) {

        Set<String> out = new HashSet<>();

        for (int i = 0; i < q.length(); i++) {

            String current = String.valueOf(q.charAt(i));

            for (String T : sigma) {

                if (current.equals(T))
                    continue;

                String candidate =
                        q.substring(0, i)
                                + T
                                + q.substring(i + 1);

                if (candidate.length() <= maxPatternLength)
                    out.add(candidate);
            }
        }

        return out;
    }

    private Set<String> applyExhaustiveSwapAll(String q) {

        Set<String> out = new HashSet<>();

        for (int i = 0; i < q.length() - 1; i++) {

            String candidate = swap(q, i);

            if (!candidate.equals(q))
                out.add(candidate);
        }

        return out;
    }

    private void activateAllExhaustiveCandidates(Set<String> candidates) {

        for (String c : candidates) {

            if (trie.exists(c))
                continue;

            if (c.length() < minLength)
                continue;

            if (c.length() > maxPatternLength)
                continue;

            if (Main.debug)
                System.out.println("Exhaustive activated pattern: " + c);

            trie.insert(c, false);
            Main.trieManager.activatePattern(c);
        }
    }
}
