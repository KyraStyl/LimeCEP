package cep;


import events.ABCEvent;
import events.KeyValueEvent;
import managers.EventManager;
import stats.Profiling;
import utils.Configs;

import java.io.IOException;
import java.util.*;

import managers.ResultManager;

public class CEPEngine {
    private String nfaLocation;
    private CEPQuery query;
    private CEPQueryParser qparser;
    private Configs configs;
    private Profiling profiling;
    private ArrayList<ArrayList<events.ABCEvent>> activeRuns;
    private ResultManager resultManager;
    private HashMap<String, CEPTransition> transitions;
    private String em_id;
    private Map<String, Set<ABCEvent>> usedPrefixesPerType = new HashMap<>();



    public  CEPEngine(){
        initializeEngine();
    }

    public CEPEngine(Configs configs, String em_id, ResultManager rm){
        this.configs = configs;
        this.nfaLocation = configs.nfaFileLocation();
        this.resultManager = rm;
        qparser = new CEPQueryParser();
        this.em_id = em_id;
        initializeEngine();
    }

    private void initializeEngine() {
        try {
            this.query = qparser.parseFromFile(this.nfaLocation);
            this.transitions = this.query.getTransitions();
            this.activeRuns = new ArrayList<>();
            this.profiling = new Profiling("our");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setResultManager(ResultManager resultManager) {
        this.resultManager = resultManager;
    }

    public void runOnce(events.ABCEvent e, EventManager<ABCEvent> eventManager) {
        Runtime runtime = Runtime.getRuntime();
        Long startTime = System.nanoTime();
        long windowTime = this.query.getTimeWindow();
        System.out.println(em_id+": window time == "+windowTime);

        long oldest_ac_timestamp = e.getTimestampDate().getTime() - windowTime;
        Date oldest_ts = new Date(oldest_ac_timestamp);

        String first = configs.first_state();
        String last = configs.last_state();

        HashMap<String, TreeSet<ABCEvent>> subsets = new HashMap<>();


        for( CEPTransition t : transitions.values()){
            String type = t.getEventType();

            if(type.equals(this.configs.last_state()))
                continue;

            TreeSet<events.ABCEvent> set = eventManager.getTreeset(type).get(type);
            if (set == null || set.isEmpty())
                return;
            set = (TreeSet<events.ABCEvent>) set.headSet(e, true);
            events.ABCEvent enew = new events.ABCEvent(e.getName()+"_temp",oldest_ts,e.getSource()+"_temp", e.getType(),e.getSymbol());

            if(set.subSet(enew,e) == null || set.subSet(enew,e).isEmpty())
                return;
            events.ABCEvent start = set.subSet(enew,e).first();
            if(start == null && !set.isEmpty())
                start = set.first();
            if (set == null)
                return;
            set = (TreeSet<events.ABCEvent>) set.subSet(start,e);

            subsets.put(type,set);

        }

        ArrayList<ABCEvent> run = new ArrayList<>();
        System.out.println(em_id+": Triggering match production process with event "+e.getName()+" @ " + e.getTimestampDate());

        if ("Skip-till-next-match".equalsIgnoreCase(configs.policy())) {
            TreeSet<ABCEvent> lastTreeset = eventManager.getTreeset(e.getSource()).get(e.getEventType());
            subsets.put(e.getEventType(),lastTreeset);
            HashMap<String, TreeSet<ABCEvent>> pruned = pruneForSTNM(e, subsets);
            if (pruned != null) {
                run.add(e);
                find_matches_once(1, pruned, run, false, false);
            }
        } else {
            run.add(e);
            find_matches_once(1, subsets, run, false, false);
        }
    }

    private HashMap<String, TreeSet<ABCEvent>> pruneForSTNM(events.ABCEvent e, HashMap<String, TreeSet<ABCEvent>> subsets) {
        HashMap<String, TreeSet<ABCEvent>> pruned = new HashMap<>();
        List<CEPTransition> transitionList = new ArrayList<>(transitions.values());
        transitionList.sort(Comparator.comparing(t -> t.getSource()));
        CEPTransition[] trans = transitionList.toArray(new CEPTransition[0]);
        int state = 0;
        int current_state_p = trans.length - 1 - state;
        events.ABCEvent lastevt = e;
        HashMap<String, HashMap<String, events.ABCEvent>> bounds = new HashMap<>();

        for (int i = current_state_p; i >= 1; i--) {
            HashMap<String, ABCEvent> cur_state_bounds = new HashMap<>();
            cur_state_bounds.put("end",lastevt);

            CEPTransition prev_state = null;
            if(i+1 < trans.length)
                prev_state = trans[i+1];
            CEPTransition current_state = trans[i];
            CEPTransition next_state = trans[i - 1];

            String prevType = null;
            if(prev_state!=null)
                prevType = prev_state.getEventType();
            String currentType = current_state.getEventType();
            String nextType = next_state.getEventType();

            TreeSet<ABCEvent> subset = subsets.get(currentType);
            TreeSet<ABCEvent> nextSubset = subsets.get(nextType);
            if(!next_state.isKleene()) {
                if(e.getName().equalsIgnoreCase("c41"))
                    System.out.println("debug");

                if (subset == null || subset.isEmpty() || nextSubset == null) return null;

                ABCEvent prevlast = null;

                  if(!current_state.isKleene())
                    if (prevType != null && bounds.containsKey(prevType) && !bounds.get(prevType).isEmpty())
                        prevlast = subset.lower(bounds.get(prevType).get("start"));
                    else
                        prevlast = subset.lower(lastevt);

                if (prevlast == null || prevlast.compareTo(nextSubset.first()) < 0)
                    prevlast = nextSubset.first();

                cur_state_bounds.put("start", prevlast);

                TreeSet<events.ABCEvent> prunedNextSet = (TreeSet<ABCEvent>) nextSubset.subSet(prevlast, e);
                if (prunedNextSet == null || prunedNextSet.isEmpty()) return null;

                TreeSet<ABCEvent> prunedNext = new TreeSet<>(prunedNextSet);
                pruned.put(nextType, prunedNext);
                bounds.put(lastevt.getEventType(), cur_state_bounds);

                lastevt = prunedNext.last(); // prepare for next step
            }else{
                pruned.put(nextType, nextSubset);
            }
        }

        return pruned;
    }


    public void runOnDemand(events.ABCEvent trigger_evt, HashMap<String, TreeSet<events.ABCEvent>> subsets){
        ArrayList<events.ABCEvent> list = new ArrayList<>();
        list.add(trigger_evt);
        find_matches_once(1,subsets,list,true,false);
    }

    private void find_matches_once(int i, HashMap<String, TreeSet<ABCEvent>> subsets, ArrayList<ABCEvent> list, boolean b, boolean b1) {
        if("Skip-till-next-match".equalsIgnoreCase(configs.policy()))
            matches_STNM(i,subsets,list,b,b1);
        else
            matches_STAM(i,subsets,list,b,b1);
    }

    public void matches_STAM(int state, HashMap<String, TreeSet<events.ABCEvent>> subsets, ArrayList<events.ABCEvent> list, boolean ooo, boolean extrun){
        List<CEPTransition> transitionList = new ArrayList<>(transitions.values());
        transitionList.sort(Comparator.comparing(t -> t.getSource()));
        CEPTransition[] trans = transitionList.toArray(new CEPTransition[0]);
        int current_state_p = trans.length - 1 - state;
        boolean stop = current_state_p < 0 ;
        boolean cantextendrun = extrun;

        if(!stop){
            CEPTransition current_state = trans[current_state_p];

            TreeSet<events.ABCEvent> subset = subsets.get(current_state.getEventType());
            if (subset == null) {
                System.out.println(em_id+": set null");
                return;
            }

            if(!subset.isEmpty()){
                if(subset.first().compareTo(list.get(0))<=0 && subset.last().compareTo(list.get(0))>=0)
                    subset = (TreeSet<events.ABCEvent>) subset.headSet(list.get(0),true);
                else if(subset.first().compareTo(list.get(0))>0)
                    subset = null;
            }

//            System.out.println(em_id + ": After headSet for type " + current_state.getEventType() + ", subset size = " + (subset != null ? subset.size() : "null"));


            if (subset == null || subset.isEmpty())
                return;

            ArrayList<events.ABCEvent> run = new ArrayList<>();

            for(events.ABCEvent e : subset.descendingSet()) {
                ArrayList<events.ABCEvent> match = new ArrayList<>(list);
                if (configs.isKleene(current_state.getEventType())) { //if is kleene

                    events.ABCEvent nextEVT = subset.higher(e);
                    if(nextEVT != null){ //if there is a higher event
                        if(current_state_p - 1 >= 0){ //if there is a previous state
                            String prevStateTag = trans[current_state_p - 1].getEventType();

                            TreeSet<events.ABCEvent> prevSet = subsets.get(prevStateTag);

                            if(prevSet!=null && prevSet.floor(e)==null)
                                continue; //was a break before, maybe should change again? !TODO:CHECK
                            if( !prevSet.floor(e).equals(prevSet.floor(nextEVT))){
                                match.addAll(0,run);
                                matches_STAM(state+1,subsets,match,ooo,!cantextendrun);
                            }
                        }
                    }
                    run.add(0,e);

                }
                else{
                    match.add(0,e);
                    matches_STAM(state + 1, subsets, match, ooo, true);
//                    if (current_state_p == 0 && !configs.isKleene(trans[current_state_p+1].getEventType())) break;
//                     if (current_state_p == 0) break; //if not kleene just add the first appropriate event from this state and stop
                    if (current_state_p == 0 && configs.isKleene(trans[current_state_p+1].getEventType()) && cantextendrun) break;
                    // (otherwise it produces all possible matches - not exactly all possible but more than maximal)
                }
            }

            if(current_state.isKleene() && !run.isEmpty()){
                list.addAll(0,run);
                matches_STAM(state+1, subsets, list, ooo,false);
            }

        }else{
            activeRuns.add(list);
            resultManager.acceptMatch(list,ooo);
//			long latency = System.nanoTime() - r.getLifeTimeBegin();
//			Profiling.updateLatency(latency);
        }
    }


    public void matches_STNM(int state, HashMap<String, TreeSet<events.ABCEvent>> subsets, ArrayList<events.ABCEvent> list, boolean ooo, boolean extrun){
        //State[] states = configs.states();
        List<CEPTransition> transitionList = new ArrayList<>(transitions.values());
        transitionList.sort(Comparator.comparing(t -> t.getSource()));
        CEPTransition[] trans = transitionList.toArray(new CEPTransition[0]);
        //CEPTransition[] trans = transitions.values().toArray(new CEPTransition[0]);
        int current_state_p = trans.length - 1 - state;
        boolean stop = current_state_p < 0 ;
        boolean cantextendrun = extrun;

        if(!stop){
            CEPTransition current_state = trans[current_state_p];

            TreeSet<events.ABCEvent> subset = subsets.get(current_state.getEventType());
            if (subset == null) {
                System.out.println(em_id+": set null");
                return;
            }

            if(!subset.isEmpty() && !list.isEmpty()){
                if(subset.first().compareTo(list.get(0))<=0 && subset.last().compareTo(list.get(0))>=0)
                    subset = (TreeSet<events.ABCEvent>) subset.headSet(list.get(0),true);
                else if(subset.first().compareTo(list.get(0))>0)
                    subset = null;
            }else
                subset = null;

            if (subset == null || subset.isEmpty())
                return;

            ArrayList<events.ABCEvent> run = new ArrayList<>();

            for(events.ABCEvent e : subset.descendingSet()) {
                ArrayList<events.ABCEvent> match = new ArrayList<>(list);
                if (configs.isKleene(current_state.getEventType())) { //if is kleene

                    events.ABCEvent nextEVT = subset.higher(e);
                    if(nextEVT != null){ //if there is a higher event
                        if(current_state_p - 1 >= 0){ //if there is a previous state
                            String prevStateTag = trans[current_state_p - 1].getEventType();

                            TreeSet<events.ABCEvent> prevSet = subsets.get(prevStateTag);

                            if(prevSet!=null && prevSet.floor(e)==null)
                                continue; //was a break before, maybe should change again? !TODO:CHECK
                            if( !prevSet.floor(e).equals(prevSet.floor(nextEVT))){
                                match.addAll(0,run);
                                matches_STNM(state+1,subsets,match,ooo,!cantextendrun);
                            }
                        }
                    }
                    run.add(0,e);

                }
                else{
                    events.ABCEvent nextEVT = subset.lower(e);
                    if(nextEVT != null){ //if there is a higher event
                        if(current_state_p - 1 >= 0){ //if there is a previous state
                            String prevStateTag = trans[current_state_p - 1].getEventType();
                            TreeSet<events.ABCEvent> prevSet = subsets.get(prevStateTag);
                            if(prevSet!=null && prevSet.floor(e)==null)
                                continue;
                            if(!prevSet.floor(e).equals(prevSet.floor(nextEVT))){
                                match.add(0,e);
                                matches_STNM(state + 1, subsets, match, ooo, false);
                            }
                        }else{
                            match.add(0,e);
                            matches_STNM(state + 1, subsets, match, ooo, false);
                            TreeSet<events.ABCEvent> nextSet = null;

                            ABCEvent prev =  subsets.get(current_state.getEventType()).lower(e);

                            String nextStateTag = trans[current_state_p + 1].getEventType();
                            nextSet = subsets.get(nextStateTag);

    //                        if(nextSet!= null && !nextSet.subSet(prev,e).isEmpty()) break;
                            if(nextSet!= null){
    //                            if(nextSet.ceiling(prev)== null || nextSet.floor(e) == null)
    //                                break;
    //                            if(nextSet.ceiling(prev).compareTo(nextSet.floor(e))<=0 && !nextSet.subSet(nextSet.ceiling(prev),nextSet.floor(e)).isEmpty())
    //                                break;
                                if(nextSet.floor(e) == null || (nextSet.ceiling(prev).compareTo(nextSet.floor(e))<=0 && !nextSet.subSet(nextSet.ceiling(prev),nextSet.floor(e)).isEmpty()))
                                    if (nextSet.floor(e) == null && e.getType().equalsIgnoreCase(configs.first_state()))
                                        continue; //was break before
                                    else
                                        break;
//                                if(current_state.isKleene()) // if everything fails - add if
                                ABCEvent lower = prev, upper = e;
                                if(lower==null || lower.compareTo(nextSet.first())<0) lower = nextSet.first();
                                if(lower.compareTo(upper)>0) upper = nextSet.last();
                                if(!nextSet.subSet(lower,upper).isEmpty()) break;
                            }
                        }
                    }else {
                        match.add(0, e);
                        matches_STNM(state + 1, subsets, match, ooo, false);
                    }
                }
            }

            if(current_state.isKleene() && !run.isEmpty()){
                list.addAll(0,run);
                matches_STNM(state+1, subsets, list, ooo,false);
            }

        }else{
            activeRuns.add(list);
            resultManager.acceptMatch(list,ooo);
        }
    }


//    public void matches_STNM(int state, HashMap<String, TreeSet<events.ABCEvent>> subsets, ArrayList<events.ABCEvent> list, boolean ooo, boolean extrun){
//        //State[] states = configs.states();
//        List<CEPTransition> transitionList = new ArrayList<>(transitions.values());
//        transitionList.sort(Comparator.comparing(t -> t.getSource()));
//        CEPTransition[] trans = transitionList.toArray(new CEPTransition[0]);
//        //CEPTransition[] trans = transitions.values().toArray(new CEPTransition[0]);
//        int current_state_p = trans.length - 1 - state;
//        boolean stop = current_state_p < 0 ;
//        boolean cantextendrun = extrun;
//
//        if(!stop){
//            CEPTransition current_state = trans[current_state_p];
//
//            TreeSet<events.ABCEvent> subset = subsets.get(current_state.getEventType());
//            if (subset == null) {
//                System.out.println(em_id+": set null");
//                return;
//            }
//
//            if(!subset.isEmpty() && !list.isEmpty()){
//                if(subset.first().compareTo(list.get(0))<=0 && subset.last().compareTo(list.get(0))>=0)
//                    subset = (TreeSet<events.ABCEvent>) subset.headSet(list.get(0),true);
//                else if(subset.first().compareTo(list.get(0))>0)
//                    subset = null;
//            }else
//                subset = null;
//
//            if (subset == null || subset.isEmpty())
//                return;
//
//            ArrayList<events.ABCEvent> run = new ArrayList<>();
//
//            for(events.ABCEvent e : subset.descendingSet()) {
//                ArrayList<events.ABCEvent> match = new ArrayList<>(list);
//                if (configs.isKleene(current_state.getEventType())) { //if is kleene
//
//                    events.ABCEvent nextEVT = subset.higher(e);
//                    if(nextEVT != null){ //if there is a higher event
//                        if(current_state_p - 1 >= 0){ //if there is a previous state
//                            String prevStateTag = trans[current_state_p - 1].getEventType();
//
//                            TreeSet<events.ABCEvent> prevSet = subsets.get(prevStateTag);
//
//                            if(prevSet!=null && prevSet.floor(e)==null)
//                                continue; //was a break before, maybe should change again? !TODO:CHECK
//                            if( !prevSet.floor(e).equals(prevSet.floor(nextEVT))){
//                                match.addAll(0,run);
//                                matches_STNM(state+1,subsets,match,ooo,!cantextendrun);
//                            }
//                        }
//                    }
//                    run.add(0,e);
//
//                }
//                else{
//                    events.ABCEvent nextEVT = subset.lower(e);
//                    if(nextEVT != null){ //if there is a "higher" event - predecessor
//                        if(current_state_p - 1 >= 0){ //if there is a previous state
//                            String prevStateTag = trans[current_state_p - 1].getEventType();
//                            TreeSet<events.ABCEvent> prevSet = subsets.get(prevStateTag);
//                            if(prevSet!=null && prevSet.floor(e)==null)
//                                continue;
//                            if(!prevSet.floor(e).equals(prevSet.floor(nextEVT))){
//                                match.add(0,e);
//                                matches_STNM(state + 1, subsets, match, ooo, false);
//                            }
//                        }else{
//                            match.add(0,e);
//                            matches_STNM(state + 1, subsets, match, ooo, false);
//                            TreeSet<events.ABCEvent> nextSet = null;
//
//                            ABCEvent prev =  subsets.get(current_state.getEventType()).lower(e);
//
//                            String nextStateTag = trans[current_state_p + 1].getEventType();
//                            nextSet = subsets.get(nextStateTag);
//
//                            //                        if(nextSet!= null && !nextSet.subSet(prev,e).isEmpty()) break;
//                            if(nextSet!= null){
//                                //                            if(nextSet.ceiling(prev)== null || nextSet.floor(e) == null)
//                                //                                break;
//                                //                            if(nextSet.ceiling(prev).compareTo(nextSet.floor(e))<=0 && !nextSet.subSet(nextSet.ceiling(prev),nextSet.floor(e)).isEmpty())
//                                //                                break;
//
//                                if(nextSet.floor(e) == null || (nextSet.ceiling(prev).compareTo(nextSet.floor(e))<=0 && !nextSet.subSet(nextSet.ceiling(prev),nextSet.floor(e)).isEmpty()))
//                                    if (e.getType().equalsIgnoreCase(configs.first_state()))
//                                        continue; //was break before
//                                    else if(!nextSet.subSet(prev,e).isEmpty()) break;
//
//
//                            }
//                        }
//                    }else {
//                        match.add(0, e);
//                        matches_STNM(state + 1, subsets, match, ooo, false);
//                    }
//                }
//            }
//
//            if(current_state.isKleene() && !run.isEmpty()){
//                list.addAll(0,run);
//                matches_STNM(state+1, subsets, list, ooo,false);
//            }
//
//        }else{
//            activeRuns.add(list);
//            resultManager.acceptMatch(list,ooo);
//        }
//    }


    public CEPQuery getQuery() {
        return query;
    }
}
