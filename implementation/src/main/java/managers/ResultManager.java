package managers;

import events.ABCEvent;
import main.Main;
import stats.Profiling;
import utils.Configs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

public class ResultManager {

    private int counter = 0;
    private ArrayList<ArrayList<ABCEvent>> matches;
    private ArrayList<Long> matchTimestamps;
    private HashMap<String, ArrayList<ArrayList<ABCEvent>>> matchesByEndEvent;

    private HashMap<Integer, Boolean> emitted;
    private HashMap<Integer, Boolean> updated;
    private HashMap<Integer, Boolean> ooo;
    private Profiling profiling;
    private String em_id;
    private Configs configs;
    private int cleanupCounter = 0;
    private boolean usesCorrection;
    private int numOfMatches = 0;
    private long lastCleanupTime = 0;


    public ResultManager(Profiling p, String em_id, Boolean withCorrection) {
        System.out.println("NEW INSTANCE OF RM");
        this.counter = 0;
        this.matches = new ArrayList<>();
        this.matchesByEndEvent = new HashMap<>();
        this.matchTimestamps = new ArrayList<>();
        this.emitted = new HashMap<>();
        this.updated = new HashMap<>();
        this.ooo = new HashMap<>();
        this.profiling = p;
        this.em_id = "R_" + em_id;
        this.usesCorrection = withCorrection;
    }

    public void acceptMatch(ArrayList<ABCEvent> m, boolean oooflag) {
        String endName = m.get(m.size() - 1).getName();
        ArrayList<ArrayList<ABCEvent>> candidates = matchesByEndEvent.getOrDefault(endName, new ArrayList<>());


        if (configs.policy().equalsIgnoreCase("Skip-till-next-match") && usesCorrection) {
            for (ArrayList<ABCEvent> existing : new ArrayList<>(matches)) {
                if (existing == null) continue;
                if (isInvalidatedBy(existing, m)) {
                    invalidateMatch(existing);
                }
                if (isInvalidatedBy(m, existing)) {
//                    System.out.println(em_id + ": Skipping match " + matchToString(m) + " (already handled by better match)");
                    return;
                }
            }
        }

        if (oooflag) {
            for (ArrayList<ABCEvent> existing : candidates) {
                if (existing == null) continue;
                if (usesCorrection) {
                    if (isMaximalCorrection(existing, m)) {
                        updateMatch(existing, m);
                        return;
                    }
                }
                if (areMatchesEquivalent(existing, m)) {
                    return;
                }
            }
        }
        long endTimestamp = m.get(m.size() - 1).getTimestamp();

        if (++cleanupCounter % 100 == 0) {
            removeExpiredMatches(endTimestamp);
            cleanupCounter = 0;
        }


        // Save the new match
        long latency = System.nanoTime() - m.get(0).getIngestionTime();
        Main.generalStats.updateLatencyProfiling(latency);

        this.matches.add(m);
        this.matchTimestamps.add(endTimestamp);
        this.matchesByEndEvent.computeIfAbsent(endName, k -> new ArrayList<>()).add(m);

        this.emitted.put(counter, true);
        this.updated.put(counter, false);
        this.ooo.put(counter, oooflag);
        this.counter++;

        if (!oooflag)
            System.out.print(em_id + ": MATCH_" + (++numOfMatches) + " ! [");
        else
            System.out.print(em_id + ": OUT-OF-ORDER MATCH_" + (++numOfMatches) + " ! [");

        m.forEach(e -> System.out.print(e.getName() + " "));
        System.out.println("] -> [" + m.get(0).getTimestampDate() + " - " + m.get(m.size() - 1).getTimestampDate() + "]");
    }

    public void updateMatch(ArrayList<ABCEvent> oldMatch, ArrayList<ABCEvent> newMatch) {
        int position = this.matches.indexOf(oldMatch);
        this.matches.set(position, newMatch);

        long newTimestamp = newMatch.get(newMatch.size() - 1).getTimestamp();
        this.matchTimestamps.set(position, newTimestamp);

        String oldEnd = oldMatch.get(oldMatch.size() - 1).getName();
        String newEnd = newMatch.get(newMatch.size() - 1).getName();

        matchesByEndEvent.getOrDefault(oldEnd, new ArrayList<>()).remove(oldMatch);
        matchesByEndEvent.computeIfAbsent(newEnd, k -> new ArrayList<>()).add(newMatch);

        this.updated.put(position, true);
        this.emitted.put(position, true);

        System.out.println(em_id + ": CORRECTING MATCH_" + (position + 1));
        System.out.print("Old match: ");
        oldMatch.forEach(e -> System.out.print(e.getName() + " "));
        System.out.println();

        System.out.print("New match: ");
        newMatch.forEach(e -> System.out.print(e.getName() + " "));
        System.out.println();
    }

    private boolean isInvalidatedBy(ArrayList<ABCEvent> oldMatch, ArrayList<ABCEvent> newMatch) {
        int minLength = Math.min(oldMatch.size(), newMatch.size());

        // Step 1: Check if prefixes match (same types & names)
        for (int i = 0; i < minLength - 1; i++) {
            ABCEvent oldE = oldMatch.get(i);
            ABCEvent newE = newMatch.get(i);
            if (!oldE.getName().equals(newE.getName())) return false;
            if (!oldE.getType().equals(newE.getType())) return false;
        }

        // Step 2: Check if old match has a different final event than new match
        ABCEvent oldLast = oldMatch.get(oldMatch.size() - 1);
        ABCEvent newLast = newMatch.get(newMatch.size() - 1);

        // Same type but different event name → possibly premature match
        return oldLast.getType().equals(newLast.getType())
                && !oldLast.getName().equals(newLast.getName())
                && oldLast.getTimestamp() > newLast.getTimestamp();
    }

    public void removeExpiredMatches(long currentTime) {
        long threshold = currentTime - 1 * configs.windowLength();

        int removed = 0;
        for (int i = 0; i < matchTimestamps.size(); i++) {
            Long ts = matchTimestamps.get(i);
            if (ts != null && ts < threshold && emitted.getOrDefault(i, false)) {
                ArrayList<ABCEvent> m = matches.get(i);
                if (m != null) {
                    String endName = m.get(m.size() - 1).getName();
                    matchesByEndEvent.getOrDefault(endName, new ArrayList<>()).remove(m);
                    matches.set(i, null);
                    matchTimestamps.set(i, null);
                }
                emitted.put(i, false);
                updated.put(i, false);
                ooo.put(i, false);
                removed++;
            }
        }
        int nullCount = (int) matchTimestamps.stream().filter(e -> e == null).count();
        if (nullCount > matchTimestamps.size() / 3) {
            compact();
        }
        System.out.println(em_id + ": REMOVED " + removed + " EXPIRED MATCHES");
    }

    private void compact() {
        ArrayList<ArrayList<ABCEvent>> newMatches = new ArrayList<>();
        ArrayList<Long> newTimestamps = new ArrayList<>();
        HashMap<Integer, Boolean> newEmitted = new HashMap<>();
        HashMap<Integer, Boolean> newUpdated = new HashMap<>();
        HashMap<Integer, Boolean> newOoo = new HashMap<>();
        HashMap<String, ArrayList<ArrayList<ABCEvent>>> newMatchesByEndEvent = new HashMap<>();

        int newIndex = 0;
        for (int i = 0; i < matches.size(); i++) {
            ArrayList<ABCEvent> m = matches.get(i);
            if (m != null) {
                newMatches.add(m);
                newTimestamps.add(matchTimestamps.get(i));
                newEmitted.put(newIndex, emitted.getOrDefault(i, false));
                newUpdated.put(newIndex, updated.getOrDefault(i, false));
                newOoo.put(newIndex, ooo.getOrDefault(i, false));

                String endName = m.get(m.size() - 1).getName();
                newMatchesByEndEvent.computeIfAbsent(endName, k -> new ArrayList<>()).add(m);

                newIndex++;
            }
        }

        this.matches = newMatches;
        this.matchTimestamps = newTimestamps;
        this.emitted = newEmitted;
        this.updated = newUpdated;
        this.ooo = newOoo;
        this.matchesByEndEvent = newMatchesByEndEvent;
        this.counter = newIndex;
    }


    public void invalidateMatch(ArrayList<ABCEvent> m) {
        int idx = matches.indexOf(m);
        if (idx != -1) {
            String endName = m.get(m.size() - 1).getName();
            matchesByEndEvent.getOrDefault(endName, new ArrayList<>()).remove(m);

            System.out.print(em_id + ": INVALIDATING PREVIOUSLY EMITTED MATCH :  ");
            m.forEach(e -> System.out.print(e.getName() + " "));
            System.out.println();

            emitted.put(idx, false);

            emitted.remove(idx);
            updated.remove(idx);
            ooo.remove(idx);
            matches.remove(idx);
        }
    }

    public void printProfiling() {
        System.out.println("This is the RM: " + em_id);
        System.out.println("I found : " + numOfMatches + " matches");
        System.out.println("I found : " + ooo.values().stream().filter(value -> value).count() + " ooo matches");
        System.out.println("-----------------------------------------");
    }

    private boolean areMatchesEquivalent(ArrayList<ABCEvent> m1, ArrayList<ABCEvent> m2) {
        if (m1.size() != m2.size()) return false;
        for (int i = 0; i < m1.size(); i++) {
            if (!m1.get(i).getName().equals(m2.get(i).getName())) return false;
        }
        return true;
    }

    private boolean isMaximalCorrection(ArrayList<ABCEvent> oldMatch, ArrayList<ABCEvent> newMatch) {
        if (oldMatch.size() >= newMatch.size()) return false;

        // Only compare if both matches have same first and last event types
        if (!oldMatch.get(0).getType().equals(newMatch.get(0).getType())) return false;
        if (!oldMatch.get(oldMatch.size() - 1).getType().equals(newMatch.get(newMatch.size() - 1).getType()))
            return false;

        // Convert old match names to a set
        Set<String> oldNames = oldMatch.stream().map(ABCEvent::getName).collect(Collectors.toSet());

        // Count how many old events exist in the new match
        long contained = newMatch.stream().filter(e -> oldNames.contains(e.getName())).count();

        return contained == oldMatch.size();
    }

    public void setConfigs(Configs configs) {
        this.configs = configs;
    }

    private String matchToString(ArrayList<ABCEvent> m) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (ABCEvent e : m) {
            sb.append(e.getName()).append(" ");
        }
        sb.append("]");
        return sb.toString();
    }
}
