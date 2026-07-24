package semi_automated;

import cep.CEPEngine;
import cep.CEPQuery;
import events.ABCEvent;
import events.TimestampComparator;
import main.Main;
import managers.ResultManager;
import operator_exploration.GlobalStats;
import operator_exploration.OperatorExplorer;
import stats.Profiling;
import utils.Configs;

import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class TrieManager {

    PatternTrie pt;
    private double thetaFreq;
    private double thetaRare;
    private OperatorExplorer explorer;
    private GlobalStats globalstats;

    public TrieManager(){
        this.pt = new PatternTrie();
        this.thetaFreq = 0.2;
        this.thetaRare = 0.01;
    }

    public TrieManager(PatternTrie pt, double thetaFreq, double thetaRare){
        this.pt = pt;
        this.thetaFreq = thetaFreq;
        this.thetaRare = thetaRare;
    }

    public void initialize(GlobalStats stats,
                           double thetaFreq,
                           double thetaRare,
                           int maxLen,
                           int budget) {

        globalstats = stats;

        explorer = new OperatorExplorer(
                pt,
                stats,
                thetaFreq,
                thetaRare,
                pt.getOriginalPattern().length(),
                maxLen,
                budget
        );
    }

    public void runOperatorBatch() {
        Set<String> activated = explorer.runBatch();
//        pt.printTrie();
    }

    public void setPt(PatternTrie pt) {
        this.pt = pt;
    }

    public PatternTrie getPt() {
        return pt;
    }

    public void setThetaFreq(double thetaFreq) {
        this.thetaFreq = thetaFreq;
    }

    public double getThetaFreq() {
        return thetaFreq;
    }

    public double getThetaRare() {
        return thetaRare;
    }

    public void setThetaRare(double thetaRare) {
        this.thetaRare = thetaRare;
    }

    public void initializeTrie(String pattern){
        pt.insert(pattern,true);
        pt.activatePattern(pattern);
        pt.printTrie(new PrintWriter(System.out,true) );
    }

    public void insertMatch(ArrayList<ABCEvent> m) {
        m.forEach(e -> System.out.print(e.getEventType()));
        String pattern = m.stream().map(e -> e.getEventType()).collect(Collectors.joining(""));
//        System.out.println("NEW PATTERN: "+m);
        pt.insert(pattern);
//        pt.printTrie();
    }

    public void evaluateEvent(String algorithm, String pattern, ABCEvent e, String window, String policy){
        String alg = algorithm.toLowerCase();
        switch (alg) {
            case "brute-force", "bruteforce", "bforce", "brute_force", "brute", "bf" -> {
                evaluate_brute_force_algorithm(pattern, e, window, policy, false);
                evaluate_brute_force_algorithm(pattern, e, window, policy, true);
            }
            case "reverse-count", "reversecount","rcount","reverse_count", "rev", "opt1", "opt-1", "optimization1", "rc" -> {
                evaluate_reverse_count_algorithm(pattern, e, window, policy, false);
                evaluate_reverse_count_algorithm(pattern, e, window, policy, true);
            }
            case "incremental-count", "incrementalcount","icount","incremental_count", "inc", "opt2", "opt-2", "optimization2", "ic" -> {
                evaluate_incremental_count_algorithm(pattern, e, window, policy, false);
                evaluate_incremental_count_algorithm(pattern, e, window, policy, true);
            }
            case "operator-explore" ->{
                evaluate_IC_explore(pattern,e, window, policy);
            }
            default -> System.out.println("Invalid algorithm: " + algorithm);
        }


    }

    private void old_evaluate_IC_explore(String pattern, ABCEvent e, String window, String policy) {

        // --------------------------------------
        // Determine prefix vs extension
        // --------------------------------------


        String Px = pattern;
        List<String> Ptypes = Px.chars()
                .mapToObj(c -> String.valueOf((char) c))
                .collect(Collectors.toList());

        int m = Ptypes.size();
        String lastType = Ptypes.get(m - 1);
        String predType = Ptypes.get(m - 2);



        CEPQuery q = new CEPQuery(pattern, null, window);

        long W = q.getTimeWindow();
        long tMin = e.getTimestampDate().getTime() - W;

        // ---------------------------------------
        // Retrieve predecessor events
        // ---------------------------------------
        String predSource = Main.typeSourceMapping.get(predType);

        char[] reversed = new StringBuilder(pattern).reverse().toString().toCharArray();
        reversed = Arrays.copyOf(reversed, reversed.length - 1);

        TreeSet<ABCEvent> lastEvtList = new TreeSet<>(new TimestampComparator());
        lastEvtList.add(e);

        Collection<ABCEvent> predSet = new ArrayList<>();
        HashMap<String, TreeSet<ABCEvent>> valid_set = new HashMap<>();

        for (char tc : reversed) {
            String type = String.valueOf(tc);

            TreeSet<ABCEvent> set = new TreeSet<>(new TimestampComparator());

            for (ABCEvent p : lastEvtList) {
                Collection<ABCEvent> preds = getPredecessorEvents(p, type, W, policy);
                set.addAll(preds);
            }
            valid_set.put(type, set);
            if(tc == reversed[reversed.length - 1])
                predSet.addAll(valid_set.get(type));
            lastEvtList = valid_set.get(type);
        }

        int sum = 0;
        for (ABCEvent ev : predSet) {
            if (Main.STS_counts.get(ev.getSource()).get(ev.getEventType()).get(ev.getName())!=null)
                sum += Main.STS_counts.get(ev.getSource()).get(ev.getEventType()).get(ev.getName());
        }

        Main.STS_counts.get(e.getSource()).get(e.getEventType()).put(e.getName(),sum);
        Main.STS.get(e.getSource()).get(lastType).add(e);

        for (int i = 0; i < sum; i++) {

            // for operator-based exploration
            double maxP = globalstats.maxP(Px);
            double minP = globalstats.minP(Px);

            if (Px.length() >= pt.getOriginalPattern().length()
                    && (maxP >= thetaFreq || minP < thetaRare)) {

                pt.insert(Px);
            }
        }

//        System.out.println("INCREMENTAL COUNT RESULT for " + Px + ": " + sum);
//        pt.printTrie();

//        suggestPatterns();
//        pt.printPatternStats();

    }

    private void evaluate_IC_explore(String pattern, ABCEvent e, String window, String policy) {

        String Px = pattern;

        CEPQuery q = new CEPQuery(pattern, null, window);
        long W = q.getTimeWindow();

        char[] reversed = new StringBuilder(pattern)
                .reverse()
                .toString()
                .substring(1)
                .toCharArray();

        List<ABCEvent> frontier = new ArrayList<>();
        frontier.add(e);

        for (char tc : reversed) {

            String type = String.valueOf(tc);

            List<ABCEvent> next = new ArrayList<>();

            for (ABCEvent p : frontier) {

                Collection<ABCEvent> preds =
                        getPredecessorEvents(p, type, W, policy);

                next.addAll(preds);
            }

            frontier = next;

            if (frontier.isEmpty())
                break;
        }

        int sum = frontier.size();

        Main.STS_counts
                .get(e.getSource())
                .get(e.getEventType())
                .put(e.getName(), sum);

        Main.STS
                .get(e.getSource())
                .get(e.getEventType())
                .add(e);

        if (sum == 0)
            return;

        double maxP = globalstats.maxP(Px);
        double minP = globalstats.minP(Px);

        if (Px.length() >= pt.getOriginalPattern().length()
                && (maxP >= thetaFreq || minP < thetaRare)) {

            pt.insertMultiple(Px, sum);
        }
    }


    private void med_evaluate_IC_explore(String pattern, ABCEvent e, String window, String policy) {

        // --------------------------------------
        // Determine prefix vs extension
        // --------------------------------------


        String Px = pattern;
        List<String> Ptypes = Px.chars()
                .mapToObj(c -> String.valueOf((char) c))
                .collect(Collectors.toList());

        int m = Ptypes.size();
        String lastType = Ptypes.get(m - 1);
        String predType = Ptypes.get(m - 2);



        CEPQuery q = new CEPQuery(pattern, null, window);

        long W = q.getTimeWindow();
        long tMin = e.getTimestampDate().getTime() - W;

        // ---------------------------------------
        // Retrieve predecessor events
        // ---------------------------------------
        String predSource = Main.typeSourceMapping.get(predType);

        char[] reversed = new StringBuilder(pattern).reverse().toString().toCharArray();
        reversed = Arrays.copyOf(reversed, reversed.length - 1);

//        TreeSet<ABCEvent> lastEvtList = new TreeSet<>(new TimestampComparator());
//        lastEvtList.add(e);
//
//        Collection<ABCEvent> predSet = new ArrayList<>();
//        HashMap<String, TreeSet<ABCEvent>> valid_set = new HashMap<>();

        List<Map.Entry<ABCEvent,Integer>> states = new ArrayList<>();
        states.add(new AbstractMap.SimpleEntry<>(e, 1));

        for (char tc : reversed) {

            String type = String.valueOf(tc);

            List<Map.Entry<ABCEvent,Integer>> nextStates = new ArrayList<>();

            for (Map.Entry<ABCEvent,Integer> state : states) {

                ABCEvent p = state.getKey();
                int multiplicity = state.getValue();

                Collection<ABCEvent> preds =
                        getPredecessorEvents(p, type, W, policy);

                for (ABCEvent pred : preds) {

                    nextStates.add(new AbstractMap.SimpleEntry<>(pred, multiplicity));

                }
            }

            states = nextStates;
        }

        int sum = 0;

        for (Map.Entry<ABCEvent,Integer> state : states) {

            ABCEvent ev = state.getKey();
            int multiplicity = state.getValue();

            Integer prefixCount =
                    Main.STS_counts
                            .get(ev.getSource())
                            .get(ev.getEventType())
                            .get(ev.getName());

            if (prefixCount != null)
                sum += multiplicity * prefixCount;
        }

        Main.STS_counts.get(e.getSource()).get(e.getEventType()).put(e.getName(),sum);
        Main.STS.get(e.getSource()).get(lastType).add(e);

        for (int i = 0; i < sum; i++) {

            // for operator-based exploration
            double maxP = globalstats.maxP(Px);
            double minP = globalstats.minP(Px);

            if (Px.length() >= pt.getOriginalPattern().length()
                    && (maxP >= thetaFreq || minP < thetaRare)) {

                pt.insert(Px);
            }
        }

//        System.out.println("INCREMENTAL COUNT RESULT for " + Px + ": " + sum);
//        pt.printTrie();

//        suggestPatterns();
//        pt.printPatternStats();

    }

    private void suggestPatterns() {
        System.out.println("=== Suggested Pattern Evolutions (Threshold: " + thetaFreq + ") ===");

        String p = pt.getOriginalPattern();
        if (p == null || p.isEmpty()) {
            System.out.println("No original pattern defined.");
            return;
        }

        // Confidence of the original pattern
        double originalConf = pt.calculateConfidence(p);

        if (originalConf >= thetaFreq || originalConf < thetaRare) {
            System.out.printf("Original Pattern %s (conf = %.2f)\n", p, originalConf);
            System.out.println("Number of matches detected: " + pt.getMatches());

            // Get extension and alteration suggestions
            List<String> exts = pt.suggestExtensions(p);
            List<String> alts = pt.suggestAlterations(p);

            // --- Print extensions with confidence ---
            System.out.println("  Extensions:");
            if (exts.isEmpty()) {
                System.out.println("    (none)");
            } else {
                for (String ext : exts) {
                    double c = pt.calculateConfidence(ext);
                    System.out.printf("    %s (conf = %.2f)\n", ext, c);
                }
            }

            // --- Print variations with confidence ---
            System.out.println("  Variations:");
            if (alts.isEmpty()) {
                System.out.println("    (none)");
            } else {
                for (String alt : alts) {
                    double c = pt.calculateConfidence(alt);
                    System.out.printf("    %s (conf = %.2f)\n", alt, c);
                }
            }

            System.out.println();
        } else {
            System.out.printf(
                    "Original pattern %s does not meet confidence threshold (%.2f < %.2f)\n",
                    p, originalConf, thetaFreq
            );
        }

        System.out.println("==============================================");
    }

    // ============================================================
    //  BRUTE FORCE ALGORITHM
    // ============================================================

    private void evaluate_brute_force_algorithm(String pattern, ABCEvent e, String window, String policy, boolean isExtension) {

        // Determine base string (pattern vs prefix)
        String base;
        if (isExtension) {
            System.out.println("PATTERN: " + pattern);
            base = pattern;                    // extension uses entire pattern
        } else {
            System.out.println("PATTERN TO EXTRACT PREFIX: " + pattern);
            base = pattern.substring(0, pattern.length() - 1);  // variation removes last char
            System.out.println("PREFIX: " + base);
        }

        // Build new pattern
        String s = base + e.getEventType();
        System.out.println(isExtension ? "PATTERN EXTENSION: " + s
                : "PATTERN VARIATION: " + s);

        String newPattern = s.chars()
                .mapToObj(c -> (char)c + " " + (char)c)
                .collect(Collectors.joining(", "));

        CEPQuery q = new CEPQuery(newPattern, null, window);
//        System.out.println(q);

        // Configs
        Configs conf = new Configs();
        conf.setNfaFileLocation("");
        conf.setStatetypes(q.getPattern());
        conf.setWindowLength((int) q.getTimeWindow());
        conf.setPolicy(policy);

        HashMap<String, TreeSet<ABCEvent>> valid_set = new HashMap<>();
        long W = conf.windowLength();

        char[] reversed = new StringBuilder(base).reverse().toString().toCharArray();

        TreeSet<ABCEvent> lastEvtList = new TreeSet<>(new TimestampComparator());
        lastEvtList.add(e);

        for (char tc : reversed) {
            String type = String.valueOf(tc);

            TreeSet<ABCEvent> set = new TreeSet<>(new TimestampComparator());

            for (ABCEvent p : lastEvtList) {
                Collection<ABCEvent> preds = getPredecessorEvents(p, type, W, policy);
                set.addAll(preds);
            }
            valid_set.put(type, set);
            lastEvtList = valid_set.get(type);
        }

        // Result Manager + CEPEngine
        String label = isExtension ? "temp_ext" : "temp_var";
        ResultManager rm = new ResultManager(new Profiling("insights"), label, false);
        rm.setConfigs(conf);

        CEPEngine cepEngine = new CEPEngine(conf, label, rm, q);
        cepEngine.runOnDemand(e, valid_set);

        rm.printProfiling();

        suggestPatterns();
    }

    // ============================================================
    //  OPTIMIZATION 1 — REVERSE COUNT
    // ============================================================

    private void evaluate_reverse_count_algorithm(
            String pattern, ABCEvent e, String window, String policy, boolean isExtension) {

        // 1. Insert event into STS
        Main.STS.get(e.getSource()).get(e.getEventType()).add(e);

        // 2. Build Px (variation or extension)
        String base = isExtension ? pattern : pattern.substring(0, pattern.length() - 1);
        String Px = base + e.getEventType();
        System.out.println("Reverse Count for " + Px);

        // 3. Determine predecessor type
        List<String> types = getPatternTypes(Px);
        int idx = types.size() - 1;
        if (idx == 0) return;   // first event type → count = 0

        String predType = types.get(idx - 1);

        // 4. Get predecessor events (identical to updateCount)
        Collection<ABCEvent> predSet =
                getPredecessorEvents(e, predType, new CEPQuery(Px, null, window).getTimeWindow(), policy);

        // 5. Recursively compute counts using SAME LOGIC as EventManager
        int total = 0;
        for (ABCEvent p : predSet)
            total += recursiveCount(Px, p, predType,
                    new CEPQuery(Px, null, window).getTimeWindow(), policy);

        System.out.println("Reverse Count = " + total);

        // 6. Insert Px into trie total times
        for (int i = 0; i < total; i++)
            pt.insert(Px);

//        pt.printTrie();
        suggestPatterns();
    }

    // ============================================================
    //  OPTIMIZATION 2 — INCREMENTAL COUNT
    // ============================================================
    private void evaluate_incremental_count_algorithm(
            String pattern, ABCEvent e, String window, String policy, boolean isExtension) {

        // --------------------------------------
        // Determine prefix vs extension
        // --------------------------------------
        String base;
        if (isExtension) {
            base = pattern;
        } else {
            base = pattern.substring(0, pattern.length() - 1);
        }

        String Px = base + e.getEventType();
        List<String> Ptypes = Px.chars()
                .mapToObj(c -> String.valueOf((char) c))
                .collect(Collectors.toList());

        int m = Ptypes.size();
        String lastType = e.getEventType();
        String predType = Ptypes.get(m - 2);

        // ---------------------------------------
        // Build CEPQuery to retrieve window size
        // ---------------------------------------
        String newPattern = Px.chars()
                .mapToObj(c -> (char) c + " " + (char) c)
                .collect(Collectors.joining(", "));

        CEPQuery q = new CEPQuery(newPattern, null, window);

        long W = q.getTimeWindow();
        long tMin = e.getTimestampDate().getTime() - W;

        // ---------------------------------------
        // Retrieve predecessor events
        // ---------------------------------------
        String predSource = Main.typeSourceMapping.get(predType);

        char[] reversed = new StringBuilder(base).reverse().toString().toCharArray();
        reversed = Arrays.copyOf(reversed, reversed.length - 1);

        TreeSet<ABCEvent> lastEvtList = new TreeSet<>(new TimestampComparator());
        lastEvtList.add(e);

        Collection<ABCEvent> predSet = new ArrayList<>();
        HashMap<String, TreeSet<ABCEvent>> valid_set = new HashMap<>();

        for (char tc : reversed) {
            String type = String.valueOf(tc);

            TreeSet<ABCEvent> set = new TreeSet<>(new TimestampComparator());

            for (ABCEvent p : lastEvtList) {
                Collection<ABCEvent> preds = getPredecessorEvents(p, type, W, policy);
                set.addAll(preds);
            }
            valid_set.put(type, set);
            if(tc == reversed[reversed.length - 1])
                predSet.addAll(valid_set.get(type));
            lastEvtList = valid_set.get(type);
        }


        // ---------------------------------------
        // Window correction and incremental count
        // ---------------------------------------
//        int sum = 0;
//        for (ABCEvent ev : predSet) {
//            int stored = Main.STS_counts.get(ev.getSource()).get(ev.getEventType()).get(ev.getName());     // You added this
//            int invalid = 0;                      // You may compute invalid based on your needs
//            int corrected = stored - invalid;
//            sum += corrected;
//        }

        int sum = 0;
        for (ABCEvent ev : predSet) {
            if (Main.STS_counts.get(ev.getSource()).get(ev.getEventType()).get(ev.getName())!=null)
                sum += Main.STS_counts.get(ev.getSource()).get(ev.getEventType()).get(ev.getName());
        }

        Main.STS_counts.get(e.getSource()).get(e.getEventType()).put(e.getName(),sum);
        Main.STS.get(e.getSource()).get(lastType).add(e);

//        for (int i = 0; i < sum; i++) {
//            // pt.insert(Px);
//
//            // for operator-based exploration
//            double maxP = globalstats.maxP(Px);
//            double minP = globalstats.minP(Px);
//
//            if (Px.length() >= pt.getOriginalPattern().length()
//                    && (maxP >= thetaFreq || minP < thetaRare)) {
//
//                pt.insert(Px);
//            }
//        }

//        System.out.println("INCREMENTAL COUNT RESULT for " + Px + ": " + sum);
//        pt.printTrie();
        suggestPatterns();
    }

    public void activatePattern(String pattern) {

//        System.out.println("ACTIVATING PATTERN " + pattern);

        // Initialize counters for incremental counting
        for(String type : pattern.split("")) {

            String source = Main.typeSourceMapping.get(type);

            Main.STS_counts
                    .get(source)
                    .computeIfAbsent(type, k -> new HashMap<>());
        }
        pt.activatePattern(pattern);
    }




    private int recursiveCount(String pattern, ABCEvent ev, String type, long W, String policy) {

        // Determine pattern order
        List<String> patternTypes = getPatternTypes(pattern);
        int idx = patternTypes.indexOf(type);

        // -----------------------
        // BASE CASE: FIRST TYPE
        // -----------------------
        if (idx == 0) {
            // count(A) = 1 (every A contributes 1 sequence)
            return 1;
        }

        // Find predecessor type
        String predType = patternTypes.get(idx - 1);

        // Predecessor events of ev
        Collection<ABCEvent> predSet = getPredecessorEvents(ev, predType, W, policy);

        // If predecessor type IS the first state,
        // then count(type) = |predSet|
        if (idx - 1 == 0) {
            return predSet.size();
        }

        // -----------------------
        // RECURSIVE CASE
        // -----------------------
        int total = 0;
        for (ABCEvent prv : predSet) {
            total += recursiveCount(pattern, prv, predType, W, policy);
        }

        return total;
    }


    private void storeCount(ABCEvent e, String type, int count) {
        Main.STS_counts
                .get(e.getSource())
                .get(type)
                .put(e.getName(), count);
    }

    private List<String> getPatternTypes(String pattern) {
        List<String> types = new ArrayList<>();
        for (char c : pattern.toCharArray()) {
            types.add(String.valueOf(c));
        }
        return types;
    }

    /*
    //previous version (almost mine)
    private Collection<ABCEvent> getPredecessorEvents(
            ABCEvent e, String predType, long W, String policy) {

        long tMin = e.getTimestampDate().getTime() - W;

        // Retrieve from correct source
        String predSource = Main.typeSourceMapping.get(predType);
        TreeSet<ABCEvent> all = Main.STS.get(predSource).get(predType);

        if (all == null || all.isEmpty())
            return Collections.emptyList();

        TreeSet<ABCEvent> candidates;

        // --- STNM logic (skip-till-next-match) ---
        if (policy.equalsIgnoreCase("skip-till-next-match")) {
            ABCEvent lower = Main.STS
                    .get(e.getSource())
                    .get(e.getEventType())
                    .lower(e);

            if (lower != null) {
                candidates = (TreeSet<ABCEvent>) all.subSet(lower, true, e, true);
            } else {
                candidates = (TreeSet<ABCEvent>) all.headSet(e, true);
            }
        }
        // --- STAM logic ---
        else {
            candidates = (TreeSet<ABCEvent>) all.headSet(e, true);
        }

        // ---- Apply time window ----
//        List<ABCEvent> filtered = new ArrayList<>();
//        for (ABCEvent ev : candidates) {
//            long ts = ev.getTimestampDate().getTime();
//            if (ts >= tMin && ts < e.getTimestampDate().getTime()) {
//                filtered.add(ev);
//            }
//        }

        ABCEvent lowerBound = new ABCEvent(
                "lb",
                new Date(tMin),
                predSource,
                predType,
                -1,
                true
        );

        ABCEvent upperBound = e;

        SortedSet<ABCEvent> filtered =
                candidates.subSet(lowerBound, upperBound);

        return filtered;
    }
*/

    private Collection<ABCEvent> getPredecessorEvents(
            ABCEvent e, String predType, long W, String policy) {

        long tMin = e.getTimestampDate().getTime() - W;

        String predSource = Main.typeSourceMapping.get(predType);
        TreeSet<ABCEvent> all = Main.STS.get(predSource).get(predType);

        if (all == null || all.isEmpty())
            return Collections.emptyList();

        // --- build window range directly on the full set ---
        ABCEvent lowerBound = new ABCEvent(
                "lb",
                new Date(tMin),
                predSource,
                predType,
                -1,
                true
        );

        NavigableSet<ABCEvent> window =
                all.subSet(lowerBound, true, e, false);

        // --- apply policy ---
        if (policy.equalsIgnoreCase("skip-till-next-match")) {

            ABCEvent lower = Main.STS
                    .get(e.getSource())
                    .get(e.getEventType())
                    .lower(e);

            if (lower != null && !window.isEmpty() && window.floor(lower) != null) {
                return window.tailSet(lower, true);
            }
        }

        return window;
    }


    private String define_pattern(boolean isExtension, ABCEvent e, String pattern){
        // --------------------------------------
        // Determine new pattern (prefix or full)
        // --------------------------------------
        String base;
        if (isExtension) {
            System.out.println("PATTERN: " + pattern);
            base = pattern;
        } else {
            System.out.println("PATTERN TO EXTRACT PREFIX: " + pattern);
            base = pattern.substring(0, pattern.length() - 1);
            System.out.println("PREFIX: " + base);
        }

        String Px = base + e.getEventType();
        System.out.println(isExtension ? "PATTERN EXTENSION: " + Px
                : "PATTERN VARIATION: " + Px);
        return Px;
    }


    public Set<String> getActivatedPatterns() {
        return this.pt.getActivatedPatterns();
    }

    public void printPTstats(PrintWriter out) {
        pt.printTrie(out);
        pt.printPatternStats(out);
    }

    public void runBatchAblation(boolean b, boolean b1, boolean b2, boolean b3, boolean b4, boolean b5) {
        explorer.runBatchAblation(b, b1, b2, b3, b4, b5);
    }

    public void runAprioriBatch(){
//        explorer.runAprioriBatch();
        explorer.runExhaustiveBatch();
    }
}
