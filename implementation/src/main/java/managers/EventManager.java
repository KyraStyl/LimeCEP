package managers;

import kafka.consumer.ConsumeInRangeMultipleTopics;
import main.Main;
import cep.CEPQuery;
import stats.Profiling;
import stats.StatisticManager;
import events.ABCEvent;
import events.Source;
import events.TimestampComparator;
import utils.ApplicationConstant;
import utils.Configs;
import cep.CEPEngine;

import java.util.*;
import java.util.stream.Collectors;

import static utils.UsefulFunctions.capitalize;

public class EventManager<T> {

    private String em_id;
    private HashMap<String, ArrayList<ABCEvent>> allEventsReceived;
    private HashMap<String, HashMap<String, TreeSet<ABCEvent>>> acceptedEventsHashlist = new HashMap<>();
    private CEPEngine cepEngine;
    private CEPQuery cepQuery;
    private StatisticManager statisticManager;
    private ResultManager resultManager;
    private Configs configs;
    private ArrayList<Source> sources;
    private Date latest_ts_arrived;
    private Date oldest_ts_arrived;
    private Profiling profiling;
    private ArrayList<String> mapping;


    public EventManager(ArrayList<Source> sources, String nfaFile, String qid, String policy, Boolean withCorrection) {
        this.em_id = "EM_" + qid;
        this.allEventsReceived = new HashMap<>();
        this.profiling = new Profiling("Our Solution");
        this.configs = new Configs();
        this.configs.setNfaFileLocation(nfaFile);
        this.configs.setPolicy(policy);
        this.resultManager = new ResultManager(this.profiling, this.em_id, withCorrection);
        this.sources = sources;

    }

    public void initializeManager() {
        this.latest_ts_arrived = null;
        this.oldest_ts_arrived = null;
        this.allEventsReceived = new HashMap<>();
        this.cepEngine = new CEPEngine(configs, em_id, resultManager);
        this.cepQuery = this.cepEngine.getQuery();
        this.cepQuery.setSources(this.sources);

        initializeConfigs();
        this.resultManager.setConfigs(configs);
    }

    private void initializeConfigs() {
        this.configs.setStatetypes(this.cepQuery.getPattern());
        this.configs.setWindowLength((int) this.cepQuery.getTimeWindow());
    }

    public void acceptEvent(String source, T event) {
        source = capitalize(source);

        if (terminate(event)) {
            this.profiling.printProfiling();
        }
        this.oldest_ts_arrived = this.oldest_ts_arrived == null ? ((ABCEvent) event).getTimestampDate() : this.oldest_ts_arrived;

        this.profiling.increaseEvents();

        double oooscore = 0;

        if (this.configs.listofStateTypes().contains(((ABCEvent) event).getEventType())) {
            oooscore = processEvent((ABCEvent) event, source, find_last(event, source), (long) (configs.windowLength()));

            if (!this.allEventsReceived.containsKey(source))
                this.allEventsReceived.put(source, new ArrayList<>());
            this.allEventsReceived.get(source).add((ABCEvent) event);
            this.latest_ts_arrived = this.latest_ts_arrived == null ?
                    ((ABCEvent) event).getTimestampDate() :
                    ((ABCEvent) event).getTimestampDate().getTime() > latest_ts_arrived.getTime() ?
                            ((ABCEvent) event).getTimestampDate() : latest_ts_arrived;
        } else return;

        if (oooscore == -2) {
            System.out.println(em_id + ": SOMETHING IS WRONG WITH THE SOURCE");
            return;
        } else if (oooscore == -1) {
            System.out.println(em_id + ": SOMETHING WENT WRONG!");
            return;
        }

        //this is an IN-ORDER event
        if (oooscore == 0) {
            if (this.configs.last_state().equals(((ABCEvent) event).getType())) {
                try {
                    cepEngine.runOnce((ABCEvent) event, (EventManager<ABCEvent>) this);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                try {
                    remove_expired_events((ABCEvent) event);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
        //this is an OUT-OF-ORDER event, but worth processing
        else if (oooscore <= statisticManager.calculateThreshold(((ABCEvent) event).getEventType())) {
            manageOutOfOrderEvent((ABCEvent) event, source);
            //create a custom kafka consumer, retrieve data within a specific time range, and check matches affected
        }
        //this is just an extremely late event.
        else {
            System.out.println(em_id + ": =====");
            System.out.println(em_id + ": An out-of-order event with very high score just arrived!");
            System.out.println(em_id + ": " + event.toString());
            System.out.println(em_id + ": " + oooscore);
            System.out.println(em_id + ": with threshold = " + statisticManager.calculateThreshold(((ABCEvent) event).getEventType()));
            System.out.println(em_id + ": =====");
        }
    }

    private boolean terminate(T event) {
        return ((ABCEvent) event).getName().equalsIgnoreCase("terminate");
    }

    private ABCEvent find_last(T event, String source) {
        TreeSet<ABCEvent> treeset = Main.STS.get(source).get(((ABCEvent) event).getEventType());
        if (treeset != null && !treeset.isEmpty())
            return treeset.last();
        else
            return (ABCEvent) event;
    }

    public HashMap<String, ArrayList<ABCEvent>> getAllEventsReceived() {
        return allEventsReceived;
    }

    public HashMap<String, TreeSet<ABCEvent>> getTreeset(String source) {
        if (!Main.typeSourceMapping.keySet().contains(source))
            return Main.STS.get(source);
        else return Main.STS.get(Main.typeSourceMapping.get(source));
    }

    //the whole pipeline for processing an incoming event
    public double processEvent(ABCEvent e, String source, ABCEvent last, Long timeWindow) {
        String type = e.getEventType();
        double timediff = Math.abs(e.getTimestampDate().getTime() - last.getTimestampDate().getTime());
        if (timediff == 0 || e.getTimestampDate().getTime() >= last.getTimestampDate().getTime() && ((latest_ts_arrived != null && e.getTimestampDate().getTime() >= latest_ts_arrived.getTime()) || latest_ts_arrived == null)) { // this event is in-order
            statisticManager.processUpdateStats(e, 0, timediff, source, false);
            return 0;
        } else {
            if (timediff >= statisticManager.estimatedArrivalRate.get(type)) {
                if (timediff >= statisticManager.actualArrivalRate.get(type)) {
                    // this is an out-of-order event for sure
                    // calculate the score
                    double score = statisticManager.calculateScore(e, source, last, timeWindow);
                    statisticManager.processUpdateStats(e, score, timediff, source, true);
                    return score;
                } else {
                    // maybe something is wrong with this source?
                    return -2;
                }
            }
        }
        return -1;
    }


    private void manageOutOfOrderEvent(ABCEvent e, String source) {
        String type = e.getEventType();

        String ls = configs.last_state();
        String last_src = Main.typeSourceMapping.get(ls);
        if (!type.equals(configs.last_state()) && (!getTreeset(last_src).get(ls).isEmpty() && e.compareTo(getTreeset(last_src).get(ls).last()) >= 0)) {
            Main.STS.get(source).get(type).add(e);
        } else {
            waitForSlack(); // let streams settle

            ABCEvent[] limits = calculateMPW(e);
            ABCEvent mpw_start = limits[0];
            ABCEvent mpw_end = limits[1];

            long W = configs.windowLength();

            // === Find candidate end events (within MPW) ===
            TreeSet<ABCEvent> allEnds = getTreeset(last_src).get(ls);
            ABCEvent from = new ABCEvent("from", mpw_start.getTimestampDate(), "", ls, -1, true);
            ABCEvent to = new ABCEvent("to", mpw_end.getTimestampDate(), "", ls, -1, true);
            SortedSet<ABCEvent> possibleEnds = allEnds.subSet(from, true, to, true);

            if (possibleEnds.isEmpty()) {
                System.out.println(em_id + ": No candidate end events in MPW");
                return;
            }

            int totalTriggered = 0;

            if (e.getType().equalsIgnoreCase(configs.last_state())) {
                possibleEnds.clear();
                possibleEnds.add(e);
            }

            for (ABCEvent endEvent : possibleEnds) {
                Date windowStart = new Date(endEvent.getTimestampDate().getTime() - W);
                ABCEvent sub_start = new ABCEvent("sub_start", windowStart, "", endEvent.getType(), -1, true);
                ABCEvent sub_end = endEvent;

                HashMap<String, Object> results = calculate_subsets(sub_start, sub_end);
                HashMap<String, Boolean> booleans = (HashMap<String, Boolean>) results.get("booleans");
                HashMap<String, TreeSet<ABCEvent>> treesets = (HashMap<String, TreeSet<ABCEvent>>) results.get("subsets");

                boolean missingData = booleans.values().stream().anyMatch(flag -> flag);

                if (missingData) {
                    System.out.println(em_id + ": I DONT HAVE THE APPROPRIATE EVENTS for end event " + endEvent.getName());

                    List<String> topics = sources.stream()
                            .filter(src -> configs.listofStateTypes().stream().anyMatch(src::hasEventType))
                            .map(Source::name).distinct().collect(Collectors.toList());

                    ConsumeInRangeMultipleTopics kfConsumer = new ConsumeInRangeMultipleTopics(
                            topics, ApplicationConstant.KAFKA_LOCAL_SERVER_CONFIG,
                            this, windowStart.getTime(), endEvent.getTimestampDate().getTime()
                    );
                    kfConsumer.setTreesets(treesets);

                    Thread t = new Thread(kfConsumer);
                    t.start();
                    try {
                        t.join();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    boolean allGood = treesets.values().stream().allMatch(set -> set != null && !set.isEmpty());
                    if (allGood) {
                        System.out.println("Triggering onDemand CEP engine with end event: " + endEvent.getName() + " due to ooo event " + e.getName());
                        cepEngine.runOnDemand(endEvent, treesets);
                        totalTriggered++;
                    }
                }
            }

            if (totalTriggered == 0) {
                System.out.println(em_id + ": No end events used – fallback to self-trigger");
                HashMap<String, Object> fallbackResults = calculate_subsets(mpw_start, mpw_end);
                HashMap<String, TreeSet<ABCEvent>> fallbackSets =
                        (HashMap<String, TreeSet<ABCEvent>>) fallbackResults.get("subsets");

                if (fallbackSets.values().stream().allMatch(set -> set != null && !set.isEmpty())) {
                    cepEngine.runOnDemand(e, fallbackSets);
                }
            }
        }
    }

    public void setStatManager(StatisticManager s) {
        this.statisticManager = s;
    }


    private void remove_expired_events(events.ABCEvent end_event) {

        ABCEvent exp_last_ = new events.ABCEvent(end_event.getName() + "_temp", new Date(end_event.getTimestampDate().getTime() - 2 * configs.windowLength()), end_event.getSource(), end_event.getType(), end_event.getSymbol());
        int count = 0;
        for (String source : Main.STS.keySet()) {
            for (String type : Main.STS.get(source).keySet()) {

                String source2 = Main.typeSourceMapping.get(type);
                TreeSet<events.ABCEvent> set = getTreeset(source2).get(type);

                if (set != null && !set.isEmpty() && exp_last_.compareTo(set.first()) >= 0) {
                    count = set.size();
                    getTreeset(source2).get(type).headSet(exp_last_).clear();
                }

            }
        }
    }

    public void accept_onDemand(HashMap<String, TreeSet<ABCEvent>> treeSetHashMap) {
        for (String key : treeSetHashMap.keySet()) {
            if (treeSetHashMap.get(key).isEmpty())
                return;
        }
        cepEngine.runOnDemand(treeSetHashMap.get(configs.last_state()).last(), treeSetHashMap);
    }

    private ABCEvent[] calculateMPW(ABCEvent e) {
        long ts = e.getTimestampDate().getTime();
        Date ts_start;
        Date ts_end;

        long Wp = configs.windowLength(); // in ms
        ArrayList<String> pattern = configs.listofStateTypes();
        int position = pattern.indexOf(e.getType());
        int nLeft = position;
        int nRight = pattern.size() - position - 1;

        long t = Wp / pattern.size();

        // Determine role of event
        boolean isFirst = e.getType().equals(configs.first_state());
        boolean isLast = e.getType().equals(configs.last_state());
        boolean isKleene = configs.isKleene(e.getType());

        if (isFirst) {
            ts_start = e.getTimestampDate();
            ts_end = new Date(Math.max(ts + Wp, ts));
        } else if (isLast) {
            ts_start = new Date(ts - Wp);
            ts_end = e.getTimestampDate();
        } else if (isKleene) {
            long kleeneOffset = Wp / 100;
            ts_start = new Date(ts - Wp + kleeneOffset);
            ts_end = new Date(ts + Wp);
        } else {
            ts_start = new Date(ts - Wp + (nRight * t));
            ts_end = new Date(Math.max(ts + Wp - (nLeft * t), latest_ts_arrived.getTime()));
        }

        // Enforce arrival-based bounds
        if (ts_start.before(oldest_ts_arrived)) {
            ts_start = oldest_ts_arrived;
        }

        if (ts_end.after(latest_ts_arrived)) {
            ts_end = latest_ts_arrived;
        }

        ABCEvent mpw_start = new ABCEvent(e.getName() + "_mpw_start", ts_start,
                e.getSource() + "_temp", e.getType(), e.getSymbol(), true);

        ABCEvent mpw_end = new ABCEvent(e.getName() + "_mpw_end", ts_end,
                e.getSource() + "_temp", e.getType(), e.getSymbol(), true);

        return new ABCEvent[]{mpw_start, mpw_end};
    }

    private HashMap<String, Object> calculate_subsets(ABCEvent start, ABCEvent end) {
        HashMap<String, Object> results = new HashMap<>();
        HashMap<String, Boolean> booleans = new HashMap<>();
        HashMap<String, TreeSet<ABCEvent>> treesets = new HashMap<>();
        results.put("booleans", booleans);
        results.put("subsets", treesets);

        long windowLength = configs.windowLength();  // pattern window in ms
        long endTs = end.getTimestampDate().getTime();  // cap based on end event
        Date windowStart = new Date(endTs - windowLength);
        Date windowEnd = end.getTimestampDate();


        for (String type : configs.listofStateTypes()) {
            String source = Main.typeSourceMapping.get(type);
            TreeSet<ABCEvent> set = getTreeset(source).get(type);
            booleans.put(type, false);

            if (set == null || set.isEmpty()) {
                booleans.put(type, true);
                treesets.put(type, new TreeSet<>(new TimestampComparator()));
                continue;
            }

            Date firstTs = set.first().getTimestampDate();
            Date lastTs = set.last().getTimestampDate();

            // Restrict subset bounds based on the actual time window AND available data
            Date lowerBound = windowStart.after(firstTs) ? windowStart : firstTs;
            Date upperBound = windowEnd.before(lastTs) ? windowEnd : lastTs;

            TreeSet<ABCEvent> subset;
            if (lowerBound.after(upperBound)) {
                subset = new TreeSet<>(new TimestampComparator());
                booleans.put(type, true);
            } else {
                ABCEvent from = new ABCEvent("from", lowerBound, "", type, -1, true);
                ABCEvent to = new ABCEvent("to", upperBound, "", type, -1, true);
                subset = new TreeSet<>(set.subSet(from, true, to, true));
                if (subset.isEmpty()) {
                    booleans.put(type, true);
                }
            }

            treesets.put(type, subset);
        }

        return results;
    }


    public String getFirstState() {
        return this.configs.first_state();
    }

    public String getLastState() {
        return this.configs.last_state();
    }

    public ArrayList<String> getQueryTypes() {

        String[] ss = cepQuery.getPattern().replace("+", "").strip().split(", ");

        for (int i = 0; i < ss.length; i++) {
            ss[i] = ss[i].split(" ")[0];
        }

        ArrayList<String> qte = new ArrayList<>();
        for (String s : ss) {
            qte.add(s);
        }
        return qte;
    }

    public CEPQuery getQuery() {
        return this.cepQuery;
    }

    public void printRMprofiling() {
        resultManager.printProfiling();
    }

    public void waitForSlack() {
        try {

            long slackMillis = statisticManager.getSlc(cepQuery.getTimeWindow());
            Thread.sleep(slackMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt flag
        }
    }

}
