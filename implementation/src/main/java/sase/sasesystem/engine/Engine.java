/*
 * Copyright (c) 2011, Regents of the University of Massachusetts Amherst 
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted 
 * provided that the following conditions are met:

 *   * Redistributions of source code must retain the above copyright notice, this list of conditions 
 * 		and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice, this list of conditions 
 * 		and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *   * Neither the name of the University of Massachusetts Amherst nor the names of its contributors 
 * 		may be used to endorse or promote products derived from this software without specific prior written 
 * 		permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR 
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF 
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package sase.sasesystem.engine;

//import cetsmanager.main.examples.Example;
//import hybridutils.CetConfig;
//import hybridutils.CetManager;
import sase.sasesystem.stream.ABCEvent;
import net.sourceforge.jeval.EvaluationException;
import sase.sasesystem.query.*;
import sase.sasesystem.stream.Event;
import sase.sasesystem.stream.Pair;
import sase.sasesystem.stream.Stream;
import utils.Configs;
import managers.EventManager;
import managers.ResultManager;

import java.io.*;
import java.util.*;

//import static hybridutils.CetManager.toBinary;


/**
 * This is the processing engine.
 * @author haopeng
 */
public class Engine {
	/**
	 * The input stream
	 */
	Stream input;
	/**
	 * The event buffer
	 */
	EventBuffer buffer;

	LinkedList<Event> mybuffer;

	ArrayList<Run> max_matches;
	ArrayList<ArrayList<Event>> runsCreated;

	HashMap<String, LinkedList<Pair>> coreStr;

	HashMap<String, Integer> expired;
	Configs configs;

	int oldest_ac_ts;
	int old_visited_run;
	String firstState;
	String finalState;

	String attribute;
	HashMap<Double,String> preds_per_state = new HashMap<>();
	/**
	 * The nfa representing the query
	 */
	NFA nfa;
	/**
	 * The run pool, which is used to reuse the run data structure.
	 */
	RunPool engineRunController;
	/**
	 * The active runs in memory
	 */
	ArrayList<Run> activeRuns;

	ArrayList<ArrayList<events.ABCEvent>> activeRunsNEW;

	HashMap<Integer, ArrayList<Run>> activeRunsByPartition;
	/**
	 * The runs which can be removed from the active runs.
	 */
	ArrayList<Run> toDeleteRuns;
	/**
	 * The matches
	 */
	MatchController matches;

	/**
	 * The buffered events for the negation components
	 */
	ArrayList<Event> negationEvents;
	
	HashMap<Integer, ArrayList<Event>> negationEventsByPartition;
	private HashMap<String, Transition> transitions;
	private boolean findAll;

	private ResultManager resultManager;

	/**
	 * The default constructor.
	 */
	
	public Engine(){
		buffer = new EventBuffer();
		engineRunController = new RunPool();
		this.activeRuns = new ArrayList<Run>();		
		this.toDeleteRuns = new ArrayList<Run>();
		this.matches = new MatchController();
		Profiling.resetProfiling();	
	
	}
	/**
	 * This method initializes the engine.
	 */
	
	public void initialize(){
		input = null;
		buffer = new EventBuffer();
		mybuffer = new LinkedList<>();
		max_matches = new ArrayList<>();
		engineRunController = new RunPool();
		coreStr = new HashMap<>();
		runsCreated = new ArrayList<>();
		oldest_ac_ts = 0;
		old_visited_run = 0;
		expired = new HashMap<>();
		firstState = this.nfa.getStates(0).getTag().replace("+","");
		finalState = this.nfa.getStates(this.nfa.getStates().length-1).getTag().replace("+","");
		if(this.configs!=null) {
			this.configs.setFirst_state(this.nfa.getStates(0).getEventType());
			this.configs.setLast_state(this.nfa.getStates(this.nfa.getStates().length - 1).getEventType());
		}
		attribute = "open_price";
		this.activeRuns = new ArrayList<Run>();
		this.activeRunsNEW = new ArrayList<>();
		this.activeRunsByPartition = new HashMap<Integer, ArrayList<Run>>();
		this.toDeleteRuns = new ArrayList<Run>();
		this.matches = new MatchController();
		Profiling.resetProfiling();	
	
	}
	/**
	 * This method is used to warm up the engine.
	 * @throws CloneNotSupportedException
	 * @throws EvaluationException
	 */
	public void warmUp() throws CloneNotSupportedException, EvaluationException{
		this.runEngine();
		buffer = new EventBuffer();
		engineRunController = new RunPool();
		this.activeRuns = new ArrayList<Run>();
		this.toDeleteRuns = new ArrayList<Run>();
		this.matches = new MatchController();
		Profiling.resetProfiling();
		
		
	}

	public void setResultManager(ResultManager resultManager) {
		this.resultManager = resultManager;
	}

	/**
	 * This is the main run logic method
	 * 
	 */
	
	public void runEngine() throws CloneNotSupportedException, EvaluationException{
		ConfigFlags.timeWindow = this.nfa.getTimeWindow();
		if(this.configs!=null)
			this.configs.setWindowLength(this.nfa.getTimeWindow());
		ConfigFlags.sequenceLength = this.nfa.getSize();
		ConfigFlags.selectionStrategy = this.nfa.getSelectionStrategy();
		ConfigFlags.hasPartitionAttribute = this.nfa.isHasPartitionAttribute();
		ConfigFlags.hasNegation = this.nfa.isHasNegation();
		System.out.println(this.nfa);

		this.configs = new Configs();
		this.configs.setIsKleene(ConfigFlags.isKleene);

		if(ConfigFlags.engine.equalsIgnoreCase("sasesystem")){
			if(ConfigFlags.hasNegation){
				this.runNegationEngine();
			}else if(ConfigFlags.selectionStrategy.equalsIgnoreCase("skip-till-any-match")){
				this.runSkipTillAnyEngine();
			}else if(ConfigFlags.selectionStrategy.equalsIgnoreCase("skip-till-next-match")){
				this.runSkipTillNextEngine();
			}else if(ConfigFlags.selectionStrategy.equalsIgnoreCase("partition-contiguity")){
				this.runPartitionContiguityEngine();
			}
//		}else if(ConfigFlags.engine.equalsIgnoreCase("cet")){
//			runCETEngine();
		}else if(ConfigFlags.engine.equalsIgnoreCase("core")){
			runCOREStructure();
		}
		
		
	}

//	private void runCETEngine() {
//		Runtime runtime = Runtime.getRuntime();
//		Long startTime = System.nanoTime();
//		ArrayList<String> eventsStr = new ArrayList<>();
//		Event e = null;
//		while ((e = this.input.popEvent())!= null){
//			eventsStr.add(e.prepareEvent());
//		}
//		int window = this.nfa.getTimeWindow();
//
//
//		String[] preds = new String[1];
//		preds[0] = getPred();
//
//		CetConfig conf = new CetConfig.Builder().type(ConfigFlags.eventtype).isSimple(true).parallelism(ConfigFlags.parallelism).inputFile(ConfigFlags.inputFile)
//				.pdfs(false).windowLength(window).preds(preds).build();
//
//		Example example = Example.getExample(conf.getConfig());
//		example.start(eventsStr);
//		Long endTime = System.nanoTime();
//		Profiling.totalRunTime =  endTime-startTime;
//		Profiling.numberOfEvents = eventsStr.size();
//		Profiling.numberOfMatches = CetManager.cets.size();
//
//		//CetManager.allComb();
//		//System.out.println("Number of distinct cets: "+ CetManager.distinct_cets.size());
//		//ArrayList<EventTrend> cets = CetManager.allComb(); // the final cets
//		//just for debug - print cets
////			for(EventTrend cet: cets){
////				System.out.println(cet.shortString());
////			}
//		try {
//			CetManager.writeToFile(false);
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		}
//
//		Profiling.memoryUsed = runtime.totalMemory() - runtime.freeMemory();
//	}


	public void runCOREStructure() throws EvaluationException {
		System.out.println("running core structure");
		this.findAll = false;
		Runtime runtime = Runtime.getRuntime();
		Long startTime = System.nanoTime();
		Event e = null;
		int window = this.nfa.getTimeWindow();

		while ((e = this.input.popEvent())!= null){
			//System.out.println(e);
			System.out.println(e.getId());
			this.buffer.bufferEvent(e);
			oldest_ac_ts = (e.getTimestamp()-window)>0? e.getTimestamp()-window:oldest_ac_ts;
			int pos = 0;
			for( String key : transitions.keySet()){
				Transition t = transitions.get(key);
				String tag = t.getSrc().getTag().replace("+","");
				if(t.getP().evaluate(e,e)){
					if(!coreStr.containsKey(tag))
						coreStr.put(tag,new LinkedList<>());
					String prevTag = findPrevTag(tag);
					if(prevTag.compareTo(firstState)>=0){
						LinkedList<Pair> prevState = coreStr.get(prevTag);
						int j=-1;
						if(prevState!=null) j+= prevState.size();
						while(j>0 && (e.getTimestamp()-prevState.get(j).event().getTimestamp())>window && e.getTimestamp()>prevState.get(j).event().getTimestamp())
							j--;
						//while(j>=0 && e.getTimestamp()<=prevState.get(j).event().getTimestamp())
						//	j--;
						pos = j;
					}
					coreStr.get(tag).add(new Pair(e,pos));
					/*for(String tgg: coreStr.keySet()){
						System.out.print("\n"+tgg +" ->");
						for(Pair p : coreStr.get(tgg)){
							System.out.print(" "+p.toString()+",");
						}
					}
					System.out.println();*/
					if( tag == finalState){ //trigger event
						ArrayList<Event> run = new ArrayList<>();
						run.add(e);
						produce_matches_with(run,tag,pos);
						deleteExpiredEvents();
					}
				}
			}
		}

		System.out.println("MAXIMAL MATCHES");
		for(Run r:activeRuns) {
			//System.out.println("MATCH -> " + r.eventIds);
//			outputMatch(new Match(r,nfa,buffer));
			if(r.eventIds.size()>Profiling.maxSizeMaximal)
				Profiling.maxSizeMaximal = r.eventIds.size();
		}

//		if(findAll) findAllMatches();

		Long endTime = System.nanoTime();
		Profiling.totalRunTime =  endTime-startTime;
		Profiling.numberOfEvents = buffer.size();
		Profiling.numberOfMatches = activeRuns.size();
		Profiling.memoryUsed = runtime.totalMemory() - runtime.freeMemory();

	}

	public void runOnce(events.ABCEvent e, EventManager<events.ABCEvent> eventManager) {
		System.out.println("SEARCHING FOR SOME MATCHES");
		Runtime runtime = Runtime.getRuntime();
		Long startTime = System.nanoTime();
		int window = this.nfa.getTimeWindow();

		long windowTime = window;
		System.out.println("window time == "+windowTime);

		long oldest_ac_timestamp = e.getTimestampDate().getTime() - windowTime;
		Date oldest_ts = new Date(oldest_ac_timestamp);

		String first = configs.first_state();
		String last = configs.last_state();

		HashMap<String, TreeSet<events.ABCEvent>> subsets = new HashMap<>();


		for( String key : configs.transitions().keySet()){
			Transition t = transitions.get(key);
			String type = t.getEventType();

			if(type.equals(this.configs.last_state()))
				continue;

			TreeSet<events.ABCEvent> set = eventManager.getTreeset(type).get(type); //TODO: fix this
			if (set == null || set.isEmpty())
				return;
			set = (TreeSet<events.ABCEvent>) set.headSet(e, true);
			events.ABCEvent enew = new events.ABCEvent(e.getName()+"_temp",oldest_ts,e.getSource()+"_temp", e.getType(),e.getSymbol());
			events.ABCEvent start = set.floor(enew);
			if(start == null && !set.isEmpty())
				start = set.first();
			if (set == null)
				return;
			set = (TreeSet<events.ABCEvent>) set.subSet(start,e);

			subsets.put(type,set);

		}

		ArrayList<events.ABCEvent> run = new ArrayList<>();
		run.add(e);
		System.out.println("Triggering match production process with event "+e.getName()+" @ " + e.getTimestampDate());
		find_matches_once(1, subsets,run,false);
//		deleteExpiredEvents();
	}

	public void find_matches_once(int state, HashMap<String, TreeSet<events.ABCEvent>> subsets, ArrayList<events.ABCEvent> list, boolean ooo){
		State[] states = configs.states();
		int current_state_p = states.length - 1 - state;
		boolean stop = current_state_p < 0 ;

		if(!stop){
			State current_state = states[current_state_p];

			TreeSet<events.ABCEvent> subset = subsets.get(current_state.getEventType());
			if (subset == null) {
				System.out.println("set null");
				return;
			}

			subset = (TreeSet<events.ABCEvent>) subset.headSet(list.get(0));

			if (subset == null || subset.isEmpty())
				return;

			ArrayList<events.ABCEvent> run = new ArrayList<>();

			for(events.ABCEvent e : subset.descendingSet()) {
				ArrayList<events.ABCEvent> match = new ArrayList<>(list);
				if (configs.isKleene(current_state.getEventType())) { //if is kleene

					events.ABCEvent nextEVT = subset.higher(e);
					if(nextEVT != null ){ //if there is a higher event
						if(current_state_p - 1 >= 0){ //if there is a previous state
							String prevStateTag = states[current_state_p - 1].getEventType();

							TreeSet<events.ABCEvent> prevSet = subsets.get(prevStateTag);

							if(!prevSet.floor(e).equals(prevSet.floor(nextEVT))){
								match.addAll(0,run);
								find_matches_once(state+1,subsets,match,ooo);
							}
						}
					}
					run.add(0,e);

				}
				else{
					match.add(0,e);
					find_matches_once(state+1, subsets, match,ooo);
					if (current_state_p == 0) break; //if not kleene just add the first appropriate event from this state and stop
					// (otherwise it produces all possible matches - can also be added as a parameter)
				}
			}

			if(configs.isKleene(current_state.getEventType()) && !run.isEmpty()){
				list.addAll(0,run);
				find_matches_once(state+1,subsets,list,ooo);
			}

		}else{
			activeRunsNEW.add(list);
			resultManager.acceptMatch(list,ooo);
//			long latency = System.nanoTime() - r.getLifeTimeBegin();
//			Profiling.updateLatency(latency);
		}
	}


	public void runCOREOnce(ABCEvent e, EventManager<events.ABCEvent> eventManager) throws EvaluationException {
		this.findAll = false;
		Runtime runtime = Runtime.getRuntime();
		Long startTime = System.nanoTime();
		int window = this.nfa.getTimeWindow();

		oldest_ac_ts = (e.getTimestamp()-window)>0? e.getTimestamp()-window:oldest_ac_ts;
		int pos = 0;
		for( String key : transitions.keySet()){
			Transition t = transitions.get(key);
			String tag = t.getSrc().getTag().replace("+","");
			if(t.getP().evaluate(e,e)){
				if(!coreStr.containsKey(tag))
					coreStr.put(tag,new LinkedList<>());
				String prevTag = findPrevTag(tag);
				if(prevTag.compareTo(firstState)>=0){
					LinkedList<Pair> prevState = coreStr.get(prevTag);
					int j=-1;
					if(prevState!=null) j+= prevState.size();
					while(j>0 && (e.getTimestamp()-prevState.get(j).event().getTimestamp())>window && e.getTimestamp()>prevState.get(j).event().getTimestamp())
						j--;
					//while(j>=0 && e.getTimestamp()<=prevState.get(j).event().getTimestamp())
					//	j--;
					pos = j;
				}
				coreStr.get(tag).add(new Pair(e,pos));
				/*for(String tgg: coreStr.keySet()){
					System.out.print("\n"+tgg +" ->");
					for(Pair p : coreStr.get(tgg)){
						System.out.print(" "+p.toString()+",");
					}
				}
				System.out.println();*/
				if( tag == finalState){ //trigger event
					ArrayList<Event> run = new ArrayList<>();
					run.add(e);
					produce_matches_with(run,tag,pos);
					deleteExpiredEvents();
				}
			}
		}

		System.out.println("MAXIMAL MATCHES");
		for(Run r:activeRuns) {
			//System.out.println("MATCH -> " + r.eventIds);
			if(r.eventIds.size()>Profiling.maxSizeMaximal)
				Profiling.maxSizeMaximal = r.eventIds.size();
		}

//		if(findAll) findAllMatches();

		Long endTime = System.nanoTime();
		Profiling.totalRunTime =  endTime-startTime;
		Profiling.numberOfEvents = buffer.size();
		Profiling.numberOfMatches = activeRuns.size();
		Profiling.memoryUsed = runtime.totalMemory() - runtime.freeMemory();

	}



//	private void findAllMatches() {
//		ArrayList<Run> toAdd = new ArrayList<>();
//		for(Run r : activeRuns) {
//			ArrayList<ArrayList<Integer>> all = findAllfromMaximal(r);
//			for (ArrayList<Integer> a : all) {
//				ArrayList<Event> temp = new ArrayList<>();
//				for(Integer id:a)
//					temp.add(this.buffer.getEvent(id));
//				Run tr = this.engineRunController.getRun();
//				tr.initializeEvents(this.nfa,temp);
//				toAdd.add(tr);
//			}
//		}
//		activeRuns.clear();
//		activeRuns.addAll(toAdd);
//
//		for(Run r: activeRuns)
//			outputMatch(new Match(r, this.nfa, this.buffer));
//	}

	private ArrayList<Integer> diff_elements(Run r, ArrayList<Integer> nonKleene){

		//System.out.println("Finding different elements");
		ArrayList<Integer> must_have = new ArrayList<>();
		must_have.addAll(nonKleene);
		int start = activeRuns.indexOf(r);
		ArrayList<Integer> remove = new ArrayList<>();
		ArrayList<Integer> already_checked = new ArrayList<>();
		boolean continue_searching = true;

		for(int rn =start-1; rn>=0 && continue_searching ;rn--) {
			Run m = activeRuns.get(rn);

			boolean search = true;

			for (int i = 0; i < nonKleene.size() && search; i++) {
				if (!m.eventIds.contains(r.eventIds.get(nonKleene.get(i))))
					search = false;
			}
			search = search && m.eventIds.get(m.eventIds.size()-1) == r.eventIds.get(r.eventIds.size()-1);

			continue_searching = r.eventIds.get(0) >= m.eventIds.get(0) && r.eventIds.get(0) <= m.eventIds.get(m.eventIds.size() - 1);

			//System.out.println("Comparing run "+r.eventIds+ " with previously found maximal match "+m.eventIds+" -> to search == "+search+" && continue searching == "+continue_searching);

			if (search && continue_searching) {
				for (int pos = 0; pos < r.eventIds.size(); pos++) {
					//System.out.println("examining event "+r.eventIds.get(pos));
					if (!m.eventIds.contains(r.eventIds.get(pos)) && !must_have.contains(pos) && !already_checked.contains(r.eventIds.get(pos))) {
						must_have.add(pos);
						//System.out.println("event "+r.eventIds.get(pos)+" missing from prev match - wasnt already added to must_have and this is the first time this event is checked");
					} else {
						if (m.eventIds.contains(r.eventIds.get(pos)) &&must_have.contains(pos) && !nonKleene.contains(pos))
							remove.add(pos);
						if (!already_checked.contains(r.eventIds.get(pos)))
							already_checked.add(r.eventIds.get(pos));
					}
				}
			}

		}
		must_have.removeAll(remove);

		return must_have;
	}

//	private ArrayList<ArrayList<Integer>> findAllfromMaximal(Run r) {
//
//		ArrayList<Integer> nonKleene = new ArrayList<>();
//		HashMap<String, Integer> counts = new HashMap<>();
//		HashMap<String, Integer> barriers = new HashMap<>();
//		for(int p=0;p<r.eventIds.size()-1;p++){
//			Event e = this.buffer.getEvent(r.eventIds.get(p));
//			String tag = preds_per_state.get(Double.valueOf(e.getAttributeByName(attribute)));
//			if ( !ConfigFlags.isKleene.get(tag)&& !nonKleene.contains(p)){
//				//System.out.println("Add "+r.eventIds.get(p)+" to non-Kleene");
//				nonKleene.add(p);
//			}else{
//				if(counts.containsKey(tag))
//					counts.put(tag, counts.get(tag)+1);
//				else {
//					counts.put(tag, 1);
//					barriers.put(tag,p); //1st event of that tag!
//					//System.out.println(r.eventIds.get(p)+" is the first event of tag "+tag);
//				}
//			}
//		}
//
//		ArrayList<Integer> must_have = diff_elements(r,nonKleene);
//
//		for(String tag:barriers.keySet())
//			if(counts.get(tag)==1){
//				must_have.add(barriers.get(tag));
//				//System.out.println("Tag "+tag+" is being represented by only one event - so event "+barriers.get(tag)+" must be in matches!");
//			}
//
//		Collections.sort(must_have);
//
//		ArrayList<ArrayList<Integer>> all = new ArrayList<>();
//		ArrayList<int[]> byteVectors = new ArrayList<>();
//		int size = r.eventIds.size();
//		int comb = 1<<size;
//		//System.out.println("event ids = "+r.eventIds);
//		//System.out.println("must have = "+must_have);
//		for(int i=1;i< comb;i++){
//			int[] temp = new int[size];
//			boolean toadd = true;
//			String t = toBinary(i,size);
//			int j = 0;
//			int count = 0;
//			for(String tk:t.split("(?!^)")){
//				temp[j] = Integer.parseInt(tk);
//				if((must_have.contains(j)|| j==size-1) && temp[j]!= 1)
//					toadd =false;
//				else if(temp[j]==1) count++;
//				j++;
//			}
//			if(toadd && count>=transitions.size())
//				byteVectors.add(temp);
//		}
//
//		for(int[] bv:byteVectors){
//
//			ArrayList<Integer> temp = new ArrayList<>();
//			boolean toAdd = true;
//			String curTag = "a";
//			int count = 0;
//			for(int pos=0; pos<bv.length; pos++) {
//				int b = bv[pos];
//				if (b == 1) {
//					temp.add(r.eventIds.get(pos));
//					count++;
//				}
//
//				if ((barriers.containsKey(curTag) && counts.containsKey(curTag))) {
//					if(pos >= barriers.get(curTag) + counts.get(curTag) -1){
//						if(count == 0){
//							toAdd = false;
//							break;
//						}
//						curTag = findNextTag(curTag);
//						count = 0;
//					}
//				}else{
//					curTag = findNextTag(curTag);
//				}
//			}
//
//			if (toAdd){
//				all.add(temp);
//			}
//		}
//
//		return all;
//
//	}

	private boolean deleteExpiredEvents(){
		int i=0, size=coreStr.size();
		int exp, prevExp=-1;
		for(String tag: coreStr.keySet()){
			exp = expired.get(tag)!=null? expired.get(tag):-1;
			deleteFromList(coreStr.get(tag),exp);
			prevExp = expired.get(findPrevTag(tag))!=null? expired.get(findPrevTag(tag)):-1;
			updateList(coreStr.get(tag),tag, prevExp);
		}
		return true;
	}

	private boolean deleteFromList(LinkedList<Pair> list, int pos){
		for(int i=0;i<pos;i++){
			try {
				list.removeFirst();
			}catch (Exception e){
				return false;
			}
		}
		return true;
	}

	private boolean updateList(LinkedList<Pair> list, String tag, int pos){
		if(pos == -1 ) return false;
		int k = 0;
		for(int i=0;i<list.size();i++){
			try {
				if(!list.get(i).changePositionBY(pos))
					k++;
			}catch (Exception e){
				return false;
			}
		}
		int exp=k;
		if(expired.get(tag)!= null)
			exp+= expired.get(tag);
		expired.put(tag,exp);
		deleteFromList(list, k);
		return true;
	}

	public void produce_matches_with(ArrayList<Event> list, String tag, int pos){
		int i=0, j;
		int minPos = -1;
		boolean notStartYet = tag.compareTo(firstState)!=0;

		if(notStartYet){
			LinkedList<Pair> prev = coreStr.get(findPrevTag(tag));
			if(prev == null || prev.size() == 0)
				return;
			while(i<prev.size() && prev.get(i).event().getTimestamp() < oldest_ac_ts){
				i++;
			}
			expired.put(findPrevTag(tag),i);
			ArrayList<Event> run = new ArrayList<>();
			for(j=pos; j>=i ; j--){
				ArrayList<Event> match = new ArrayList<>(list);
				Event evt = prev.get(j).event();
				//TODO: need to check for further predicates regarding the accepting events - not just accept the events
				int posJ = prev.get(j).position();
				minPos = minPos < 0? posJ : minPos;

				if(this.configs.isKleene(findPrevTag(tag))){//prev tag is kleene
					if(posJ < minPos){
						match.addAll(0,run);
						produce_matches_with(match, findPrevTag(tag), minPos);
						minPos = posJ;
					}
					run.add(0,evt);
				}else{//prev tag not kleene
					match.add(0,evt);
					produce_matches_with(match, findPrevTag(tag), posJ);
				}
			}
			if(this.configs.isKleene(findPrevTag(tag)) && run.size()!=0){
				list.addAll(0,run);
				produce_matches_with(list, findPrevTag(tag), minPos);
			}
		}
		else{
			Run r = this.engineRunController.getRun();
			r.initializeEvents(this.nfa, list);
			r.setLifeTimeBegin(list.get(0).arrivalTime());
			activeRuns.add(r);
			long latency = System.nanoTime() - r.getLifeTimeBegin();
			Profiling.updateLatency(latency);
			if(!findAll) outputMatch(new Match(r, this.nfa, this.buffer));
		}
	}

	public String findPrevTag(String tag){
		return String.valueOf((char)(((tag.charAt(0) - 'a') - 1)+'a'));
	}

	public String findNextTag(String tag){
		return String.valueOf((char)(((tag.charAt(0) - 'a') + 1)+'a'));
	}

	public Run newRun(Event e){
		Run m = this.engineRunController.getRun();
		m.initializeRun(this.nfa);
		m.addEvent(e);
		max_matches.add(m);
		return m;
	}

	private String getPred() {
		String line = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(ConfigFlags.queryFile));
			while((line = br.readLine())!=null){
				if(line.startsWith("AND"))
					break;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return line.split("AND ")[1];
	}


	/**
	 * The main method for skip-till-any-match
	 * @throws CloneNotSupportedException
	 * @throws EvaluationException
	 */
	public void runSkipTillAnyEngine() throws CloneNotSupportedException, EvaluationException{
		if(!ConfigFlags.hasPartitionAttribute){
			Event e = null;
			long currentTime = 0;
			while ((e = this.input.popEvent())!= null){// evaluate event one by one
				System.out.println(e.getId());
				currentTime = System.nanoTime();
				this.evaluateRunsForSkipTillAny(e);// evaluate existing runs
				if(this.toDeleteRuns.size() > 0){
					this.cleanRuns();
				}
				this.createNewRun(e);// create new run starting with this event if possible
				
				
				Profiling.totalRunTime += (System.nanoTime() - currentTime);
				Profiling.numberOfEvents += 1;
				
			}
		}
		
		if(ConfigFlags.hasPartitionAttribute){
			ConfigFlags.partitionAttribute = this.nfa.getPartitionAttribute();
			this.activeRunsByPartition = new HashMap<Integer, ArrayList<Run>>();
			Event e = null;
			long currentTime = 0;
			while ((e = this.input.popEvent())!= null){// evaluate event one by one
				currentTime = System.nanoTime();
				this.evaluateRunsByPartitionForSkipTillAny(e);// evaluate existing runs
				if(this.toDeleteRuns.size() > 0){
					this.cleanRunsByPartition();
				}
				this.createNewRunByPartition(e);// create new run starting with this event if possible
				
				
				Profiling.totalRunTime += (System.nanoTime() - currentTime);
				Profiling.numberOfEvents += 1;
				
			}
		}
	}

	public void runOnceSTAMnoPartition(events.ABCEvent e, stats.Profiling profiling) throws CloneNotSupportedException,EvaluationException{
		long currentTime = System.nanoTime();
		this.evaluateRunsForSkipTillAny(e, profiling);// evaluate existing runs
		if(this.toDeleteRuns.size() > 0){
			this.cleanRuns();
		}
		this.createNewRun(e);// create new run starting with this event if possible


		Profiling.totalRunTime += (System.nanoTime() - currentTime);
		Profiling.numberOfEvents += 1;
	}


	/**
	 * The main method for skip-till-next-match
	 * @throws CloneNotSupportedException
	 * @throws EvaluationException
	 */
	public void runSkipTillNextEngine() throws CloneNotSupportedException, EvaluationException{
		if(!ConfigFlags.hasPartitionAttribute){
			Event e = null;
			long currentTime = 0;
			while ((e = this.input.popEvent())!= null){// evaluate event one by one
				System.out.println("evaluating event = "+e);
				currentTime = System.nanoTime();
				this.evaluateRunsForSkipTillNext(e);// evaluate existing runs
				if(this.toDeleteRuns.size() > 0){
					this.cleanRuns();
				}
				this.createNewRun(e);// create new run starting with this event if possible
				
				
				Profiling.totalRunTime += (System.nanoTime() - currentTime);
				Profiling.numberOfEvents += 1;
				
			}
		}
		
		if(ConfigFlags.hasPartitionAttribute){
			ConfigFlags.partitionAttribute = this.nfa.getPartitionAttribute();
			this.activeRunsByPartition = new HashMap<Integer, ArrayList<Run>>();
			Event e = null;
			long currentTime = 0;
			while ((e = this.input.popEvent())!= null){// evaluate event one by one
				currentTime = System.nanoTime();
				this.evaluateRunsByPartitionForSkipTillNext(e);// evaluate existing runs
				if(this.toDeleteRuns.size() > 0){
					this.cleanRunsByPartition();
				}
				this.createNewRunByPartition(e);// create new run starting with this event if possible
				
				
				Profiling.totalRunTime += (System.nanoTime() - currentTime);
				Profiling.numberOfEvents += 1;
				
			}
		}
	}
	
	/**
	 * This method is called when the query uses the partition-contiguity selection strategy
	 * @throws CloneNotSupportedException 
	 *  
	 */
	
	public void runPartitionContiguityEngine() throws EvaluationException, CloneNotSupportedException{
		
		ConfigFlags.partitionAttribute = this.nfa.getPartitionAttribute();
		ConfigFlags.hasPartitionAttribute = true;
		this.activeRunsByPartition = new HashMap<Integer, ArrayList<Run>>();
		
		ConfigFlags.timeWindow = this.nfa.getTimeWindow();
		ConfigFlags.sequenceLength = this.nfa.getSize();
		ConfigFlags.selectionStrategy = this.nfa.getSelectionStrategy();
		
		Event e = null;
		long currentTime = 0;
		while((e = this.input.popEvent())!= null){
			
			currentTime = System.nanoTime();
			this.evaluateRunsForPartitionContiguity(e);
			if(this.toDeleteRuns.size() > 0){
				this.cleanRunsByPartition();
			}
			this.createNewRunByPartition(e);
			Profiling.totalRunTime += (System.nanoTime() - currentTime);
			Profiling.numberOfEvents += 1;
		}
	}
	/**
	 * The main method when there is a negation component in the query
	 * @throws CloneNotSupportedException
	 * @throws EvaluationException
	 */
	public void runNegationEngine() throws CloneNotSupportedException, EvaluationException{
		if(!ConfigFlags.hasPartitionAttribute){
			Event e = null;
			long currentTime = 0;
			this.negationEvents = new ArrayList<Event>();
			while ((e = this.input.popEvent())!= null){// evaluate event one by one
				currentTime = System.nanoTime();
				if(this.checkNegation(e)){
					this.negationEvents.add(e);
				}else{
					this.evaluateRunsForNegation(e);// evaluate existing runs
					if(this.toDeleteRuns.size() > 0){
						this.cleanRuns();
					}
					this.createNewRun(e);// create new run starting with this event if possible
				}
				
				Profiling.totalRunTime += (System.nanoTime() - currentTime);
				Profiling.numberOfEvents += 1;
				
			}
		}
		
		if(ConfigFlags.hasPartitionAttribute){
			ConfigFlags.partitionAttribute = this.nfa.getPartitionAttribute();
			this.activeRunsByPartition = new HashMap<Integer, ArrayList<Run>>();
			this.negationEventsByPartition = new HashMap<Integer, ArrayList<Event>>();
			
			Event e = null;
			long currentTime = 0;
			while ((e = this.input.popEvent())!= null){// evaluate event one by one
				currentTime = System.nanoTime();
				if(this.checkNegation(e)){
					this.indexNegationByPartition(e);
				}else{
					this.evaluateRunsByPartitionForNegation(e);// evaluate existing runs
					if(this.toDeleteRuns.size() > 0){
						this.cleanRunsByPartition();
					}
					this.createNewRunByPartition(e);// create new run starting with this event if possible
				}
				
				Profiling.totalRunTime += (System.nanoTime() - currentTime);
				Profiling.numberOfEvents += 1;
				
			}
		}
	}
	/**
	 * This method will iterate all existing runs for the current event, for skip-till-any-match.
	 * @param e The current event which is being evaluated.
	 * @throws CloneNotSupportedException
	 * @throws EvaluationException
	 */
		
		public void evaluateRunsForSkipTillAny(Event e) throws CloneNotSupportedException, EvaluationException{
			int size = this.activeRuns.size();
			for(int i = 0; i < size; i ++){
				Run r = this.activeRuns.get(i);
					if(r.isFull()){
						continue;
					}
					this.evaluateEventForSkipTillAny(e, r);
				}
		}

		public void evaluateRunsForSkipTillAny(events.ABCEvent e, stats.Profiling profiling) throws CloneNotSupportedException, EvaluationException{
			int size = this.activeRuns.size();
			for(int i = 0; i < size; i ++){
				Run r = this.activeRuns.get(i);
				if(r.isFull()){
					continue;
				}
				this.evaluateEventForSkipTillAny(e, r, profiling);
			}
		}

	/**
		 * This method will iterate all existing runs for the current event, for skip-till-next-match.
		 * @param e The current event which is being evaluated.
		 * @throws CloneNotSupportedException
		 * @throws EvaluationException
		 */
			
			public void evaluateRunsForSkipTillNext(Event e) throws CloneNotSupportedException, EvaluationException{
				int size = this.activeRuns.size();
				for(int i = 0; i < size; i ++){
					Run r = this.activeRuns.get(i);
						if(r.isFull()){
							continue;
						}
						this.evaluateEventForSkipTillNext(e, r);
					}
			}
			/**
			 * This method will iterate all existing runs for the current event, for queries with a negation component.
			 * @param e The current event which is being evaluated.
			 * @throws CloneNotSupportedException
			 * @throws EvaluationException
			 */
			public void evaluateRunsForNegation(Event e) throws CloneNotSupportedException, EvaluationException{
				int size = this.activeRuns.size();
				for(int i = 0; i < size; i ++){
					Run r = this.activeRuns.get(i);
						if(r.isFull()){
							continue;
						}
						this.evaluateEventForNegation(e, r);
					}
			}

	
	/**
	 * This method will iterate runs in the same partition for the current event, for skip-till-any-match
	 * @param e The current event which is being evaluated.
	 * @throws CloneNotSupportedException
	 */
		public void evaluateRunsByPartitionForSkipTillAny(Event e) throws CloneNotSupportedException{
			int key = e.getAttributeByName(ConfigFlags.partitionAttribute);
			if(this.activeRunsByPartition.containsKey(key)){
				ArrayList<Run> partitionedRuns = this.activeRunsByPartition.get(key);
				int size = partitionedRuns.size();
				for(int i = 0; i < size; i ++){
					Run r = partitionedRuns.get(i);
					if(r.isFull()){
						continue;
					}
					this.evaluateEventOptimizedForSkipTillAny(e, r);//
				}
			}
		}
		
		/**
		 * This method will iterate runs in the same partition for the current event, for skip-till-next-match
		 * @param e The current event which is being evaluated.
		 * @throws CloneNotSupportedException
		 */
			public void evaluateRunsByPartitionForSkipTillNext(Event e) throws CloneNotSupportedException{
				int key = e.getAttributeByName(ConfigFlags.partitionAttribute);
				if(this.activeRunsByPartition.containsKey(key)){
					ArrayList<Run> partitionedRuns = this.activeRunsByPartition.get(key);
					int size = partitionedRuns.size();
					for(int i = 0; i < size; i ++){
						Run r = partitionedRuns.get(i);
						if(r.isFull()){
							continue;
						}
						this.evaluateEventOptimizedForSkipTillNext(e, r);//
					}
				}
			}
			/**
			 * This method will iterate runs in the same partition for the current event, for queries with a negation component.
			 * @param e The current event which is being evaluated.
			 * @throws CloneNotSupportedException
			 */
			public void evaluateRunsByPartitionForNegation(Event e) throws CloneNotSupportedException{
				int key = e.getAttributeByName(ConfigFlags.partitionAttribute);
				if(this.activeRunsByPartition.containsKey(key)){
					ArrayList<Run> partitionedRuns = this.activeRunsByPartition.get(key);
					int size = partitionedRuns.size();
					for(int i = 0; i < size; i ++){
						Run r = partitionedRuns.get(i);
						if(r.isFull()){
							continue;
						}
						this.evaluateEventOptimizedForNegation(e, r);//
					}
				}
			}

	/**
	 * If the selection strategy is partition-contiguity, this method is called and it will iterate runs in the same partition for the current event
	 * @param e The current event which is being evaluated.
	 * @throws CloneNotSupportedException
	 */
	public void evaluateRunsForPartitionContiguity(Event e) throws CloneNotSupportedException{
		int key = e.getAttributeByName(ConfigFlags.partitionAttribute);
		if(this.activeRunsByPartition.containsKey(key)){
			ArrayList<Run> partitionedRuns = this.activeRunsByPartition.get(key);
			int size = partitionedRuns.size();
			for(int i = 0; i < size; i ++){
				Run r = partitionedRuns.get(i);
				if(r.isFull()){
					continue;
				}
				this.evaluateEventForPartitonContiguityOptimized(e, r);//
			}
		}
	}
	

	/**
	 * This method evaluates the event for a given run, for skip-till-any-match
	 * @param e The current event which is being evaluated.
	 * @param r The run against which the evaluation goes
	 * @throws CloneNotSupportedException 
	 */
	public void evaluateEventOptimizedForSkipTillAny(Event e, Run r) throws CloneNotSupportedException{
		int checkResult = this.checkPredicateOptimized(e, r);
		switch(checkResult){
			case 1:
				boolean timeWindow = this.checkTimeWindow(e, r);
				if(timeWindow){
					Run newRun = this.cloneRun(r);
					this.addRunByPartition(newRun);
					
					this.addEventToRun(r, e);
				}else{
					this.toDeleteRuns.add(r);
				}
				break;
			case 2:
				Run newRun = this.cloneRun(r);
				this.addRunByPartition(newRun);
				  
				r.proceed();
				this.addEventToRun(r, e);
		}
	}
	/**
	 * This method evaluates the event for a given run, for skip-till-next-match.
	 * @param e The current event which is being evaluated.
	 * @param r The run against which the evaluation goes
	 */
	public void evaluateEventOptimizedForSkipTillNext(Event e, Run r){
		int checkResult = this.checkPredicateOptimized(e, r);
		switch(checkResult){
			case 1:
				boolean timeWindow = this.checkTimeWindow(e, r);
				if(timeWindow){
					this.addEventToRun(r, e);
				}else{
					this.toDeleteRuns.add(r);
				}
				break;
			case 2:
				r.proceed();
				this.addEventToRun(r, e);
		}
	}
	/**
	 * This method evaluates the event for a given run, for queries with a negation component.
	 * @param e The current event which is being evaluated.
	 * @param r The run against which the evaluation goes
	 */
	public void evaluateEventOptimizedForNegation(Event e, Run r) throws CloneNotSupportedException{
		int checkResult = this.checkPredicateOptimized(e, r);
		switch(checkResult){
			case 1:
				boolean timeWindow = this.checkTimeWindow(e, r);
				if(timeWindow){
					Run newRun = this.cloneRun(r);
					this.addRunByPartition(newRun);
					
					this.addEventToRunForNegation(r, e);
				}else{
					this.toDeleteRuns.add(r);
				}
				break;
			case 2:
				Run newRun = this.cloneRun(r);
				this.addRunByPartition(newRun);
				  
				r.proceed();
				this.addEventToRunForNegation(r, e);
		}
	}
	/**
	 * If the selection strategy is partition-contiguity, this method is called, and it evaluates the event for a given run
	 * @param e The current event which is being evaluated
	 * @param r The run against which the evaluation goes
	 * @throws CloneNotSupportedException
	 */
	public void evaluateEventForPartitonContiguityOptimized(Event e, Run r) throws CloneNotSupportedException{
		int checkResult = this.checkPredicateOptimized(e, r);
		switch(checkResult){
			case 0: 
				this.toDeleteRuns.add(r); 
				break;
			case 1:
				boolean timeWindow = this.checkTimeWindow(e, r);
				if(timeWindow){
					this.addEventToRun(r, e);
				}else{
					this.toDeleteRuns.add(r);
				}
				break;
			case 2:
				r.proceed();
				this.addEventToRun(r, e);
		}
		
		
}

	/**
	 * This method evaluates an event against a run, for skip-till-any-match
	 * @param e The event which is being evaluated.
	 * @param r The run which the event is being evaluated against.
	 * @throws CloneNotSupportedException
	 */
	public void evaluateEventForSkipTillAny(Event e, Run r) throws CloneNotSupportedException{
		boolean checkResult = true;
		
		
		checkResult = this.checkPredicate(e, r);// check predicate
			if(checkResult){ // the predicate if ok.
				checkResult = this.checkTimeWindow(e, r); // the time window is ok
				if(checkResult){// predicate and time window are ok
					this.buffer.bufferEvent(e);// add the event to buffer
					int oldState = 0;
					int newState = 0;

					Run newRun = this.cloneRun(r); // clone this run

					oldState = newRun.getCurrentState();
					newRun.addEvent(e);					// add the event to this run
					newState = newRun.getCurrentState();
					if(oldState != newState){
						this.activeRuns.add(newRun);
					}else{//kleene closure
						if(newRun.isFull){
							//check match and output match
							if(newRun.checkMatch()){
								this.outputMatch(new Match(newRun, this.nfa, this.buffer));
								long latency = System.nanoTime() - r.getLifeTimeBegin();
								Profiling.updateLatency(latency);
								Profiling.totalRunLifeTime += latency;
							}
						}else{
							//check proceed
							if(this.checkProceed(newRun)){
								Run newerRun = this.cloneRun(newRun);
								this.activeRuns.add(newRun);
								newerRun.proceed();
								if(newerRun.isComplete()){
									this.outputMatch(new Match(r, this.nfa, this.buffer));
									long latency = System.nanoTime() - r.getLifeTimeBegin();
									Profiling.updateLatency(latency);
									Profiling.totalRunLifeTime += latency;
								}else {
									this.activeRuns.add(newerRun);
								}
							}
						}
					}
				}else{
					this.toDeleteRuns.add(r);
				}

				}
	}

	public void evaluateEventForSkipTillAny(events.ABCEvent e, Run r, stats.Profiling profiling) throws CloneNotSupportedException{
		boolean checkResult = true;


		checkResult = this.checkPredicate(e, r);// check predicate
		if(checkResult){ // the predicate if ok.
			checkResult = this.checkTimeWindow(e, r); // the time window is ok
			if(checkResult){// predicate and time window are ok
				this.buffer.bufferEvent(e);// add the event to buffer
				int oldState = 0;
				int newState = 0;

				Run newRun = this.cloneRun(r); // clone this run

				oldState = newRun.getCurrentState();
				newRun.addEvent(e);					// add the event to this run
				newState = newRun.getCurrentState();
				if(oldState != newState){
					this.activeRuns.add(newRun);
				}else{//kleene closure
					if(newRun.isFull){
						//check match and output match
						if(newRun.checkMatch()){
							long latency = System.nanoTime() - r.getLifeTimeBegin();
							this.outputMatch(new Match(newRun, this.nfa, this.buffer),latency);
							profiling.updateProfiling(latency);
							Profiling.updateLatency(latency);
							Profiling.totalRunLifeTime += latency;
						}
					}else{
						//check proceed
						if(this.checkProceed(newRun)){
							Run newerRun = this.cloneRun(newRun);
							this.activeRuns.add(newRun);
							newerRun.proceed();
							if(newerRun.isComplete()){
								long latency = System.nanoTime() - r.getLifeTimeBegin();
								this.outputMatch(new Match(r, this.nfa, this.buffer),latency);
								profiling.updateProfiling(latency);
								Profiling.updateLatency(latency);
								Profiling.totalRunLifeTime += latency;
							}else {
								this.activeRuns.add(newerRun);
							}
						}
					}
				}
			}else{
				this.toDeleteRuns.add(r);
			}

		}
	}
	/**
	 * This method evaluates an event against a run, for skip-till-next-match
	 * @param e The event which is being evaluated.
	 * @param r The run which the event is being evaluated against.
	 * @throws CloneNotSupportedException
	 */
	public void evaluateEventForSkipTillNext(Event e, Run r) throws CloneNotSupportedException{
		boolean checkResult = true;
		
		checkResult = this.checkPredicate(e, r);// check predicate
			if(checkResult){ // the predicate if ok.
				checkResult = this.checkTimeWindow(e, r); // the time window is ok
				if(checkResult){// predicate and time window are ok
					this.buffer.bufferEvent(e);// add the event to buffer
					int oldState = 0;
					int newState = 0;
					
					
						oldState = r.getCurrentState();
						r.addEvent(e);
						newState = r.getCurrentState();
						if(oldState == newState)//kleene closure
							if(r.isFull){
								//check match and output match
								if(r.checkMatch()){
									this.outputMatch(new Match(r, this.nfa, this.buffer));
									long latency = (System.nanoTime() - r.getLifeTimeBegin());
									Profiling.updateLatency(latency);
									Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
									this.toDeleteRuns.add(r);
								}
							}else{
								//check proceed
								if(this.checkProceed(r)){
									Run newRun = this.cloneRun(r);
									
									this.activeRuns.add(newRun);
									this.addRunByPartition(newRun);
									r.proceed();
									if(r.isComplete()){
										this.outputMatch(new Match(r, this.nfa, this.buffer));
										Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
										long latency = (System.nanoTime() - r.getLifeTimeBegin());
										Profiling.updateLatency(latency);
										this.toDeleteRuns.add(r);
										
									}
								}
							}
					}else{
						this.toDeleteRuns.add(r);
					}

				}
	}
	/**
	 * This method evaluates an event against a run, for queries with a negation component.
	 * @param e The event which is being evaluated.
	 * @param r The run which the event is being evaluated against.
	 * @throws CloneNotSupportedException
	 */
	public void evaluateEventForNegation(Event e, Run r) throws CloneNotSupportedException{
		boolean checkResult = true;
		
		
		checkResult = this.checkPredicate(e, r);// check predicate
			if(checkResult){ // the predicate if ok.
				checkResult = this.checkTimeWindow(e, r); // the time window is ok
				if(checkResult){// predicate and time window are ok
					this.buffer.bufferEvent(e);// add the event to buffer
					int oldState = 0;
					int newState = 0;
					
					
						Run newRun = this.cloneRun(r); // clone this run
						
						
						oldState = newRun.getCurrentState();
						newRun.addEvent(e);					// add the event to this run
						newState = newRun.getCurrentState();
						if(oldState != newState){
							this.activeRuns.add(newRun);
							State tempState = this.nfa.getStates(newState);
							if(tempState.isBeforeNegation()){
								r.setBeforeNegationTimestamp(e.getTimestamp());
							}else if(tempState.isAfterNegation()){
								r.setAfterNegationTimestamp(e.getTimestamp());
							}
						}else{//kleene closure
							if(newRun.isFull){
								//check match and output match
								if(newRun.checkMatch()){
									this.outputMatchForNegation(new Match(newRun, this.nfa, this.buffer), newRun);
									Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
															
								}
							}else{
								//check proceed
								if(this.checkProceed(newRun)){
									Run newerRun = this.cloneRun(newRun);
									this.activeRuns.add(newRun);
									newerRun.proceed();
									if(newerRun.isComplete()){
										this.outputMatchForNegation(new Match(r, this.nfa, this.buffer), r);
										Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
										
									}else {
										this.activeRuns.add(newerRun);
									}
								}
							}
						}
						
					
						
					}else{
						this.toDeleteRuns.add(r);
					}

				}
	}
	/**
	 * This methods add a new run to a partition.
	 * @param newRun The run to be added
	 */
		
	public void addRunByPartition(Run newRun){
		if(this.activeRunsByPartition.containsKey(newRun.getPartitonId())){
			this.activeRunsByPartition.get(newRun.getPartitonId()).add(newRun);
		}else{
			ArrayList<Run> newPartition = new ArrayList<Run>();
			newPartition.add(newRun);
			this.activeRunsByPartition.put(newRun.getPartitonId(), newPartition);
		}
	}
	
	/**
	 * This method evaluates an event against a run.
	 * @param e The event which is being evaluated.
	 * @param r The run which the event is being evaluated against.
	 * @throws CloneNotSupportedException
	 */
		
		
		public void evaluateEventForPartitonContiguity(Event e, Run r) throws CloneNotSupportedException{
			boolean checkResult = true;
			
			
			checkResult = this.checkPredicate(e, r);// check predicate
				if(checkResult){ // the predicate if ok.
					checkResult = this.checkTimeWindow(e, r); // the time window is ok
					if(checkResult){// predicate and time window are ok
						this.buffer.bufferEvent(e);// add the event to buffer
						int oldState = 0;
						int newState = 0;
						oldState = r.getCurrentState();
						r.addEvent(e);
						newState = r.getCurrentState();
						if(oldState == newState)//kleene closure
							if(r.isFull){
								//check match and output match
								if(r.checkMatch()){
									this.outputMatch(new Match(r, this.nfa, this.buffer));
									Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
									this.toDeleteRuns.add(r);
								}
							}else{
								//check proceed
								if(this.checkProceed(r)){
									Run newRun = this.cloneRun(r);
									this.activeRuns.add(newRun);
									this.addRunByPartition(newRun);
									
									r.proceed();
									if(r.isComplete()){
										this.outputMatch(new Match(r, this.nfa, this.buffer));
										Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
										this.toDeleteRuns.add(r);
										
									}
								}
							}
						
							
						}else{
							this.toDeleteRuns.add(r);
						}

					}else{
						this.toDeleteRuns.add(r);
					}
	}
	

	/**
	 * This method adds an event to a run
	 * @param r The event to be added
	 * @param e The run to which the event is added
	 */
	public void addEventToRun(Run r, Event e){
		this.buffer.bufferEvent(e);// add the event to buffer
		int oldState = 0;
		int newState = 0;
		oldState = r.getCurrentState();
		r.addEvent(e);
		newState = r.getCurrentState();
		if(oldState == newState)//kleene closure
			if(r.isFull){
				//check match and output match
				if(r.checkMatch()){
					this.outputMatch(new Match(r, this.nfa, this.buffer));
					Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
					this.toDeleteRuns.add(r);
				}
			}
		
	}
	/**
	 * This method adds an event to a run, for queries with a negation component.
	 * @param r The event to be added
	 * @param e The run to which the event is added
	 */
	public void addEventToRunForNegation(Run r, Event e){
		this.buffer.bufferEvent(e);// add the event to buffer
		int oldState = 0;
		int newState = 0;
		oldState = r.getCurrentState();
		r.addEvent(e);
		newState = r.getCurrentState();
		State tempState = this.nfa.getStates(newState);
		
		if(tempState.isBeforeNegation()){
			r.setBeforeNegationTimestamp(e.getTimestamp());
		}else if(tempState.isAfterNegation()){
			r.setAfterNegationTimestamp(e.getTimestamp());
		}
		if(oldState == newState)//kleene closure
			if(r.isFull){
				//check match and output match
				if(r.checkMatch()){
					this.outputMatchByPartitionForNegation(new Match(r, this.nfa, this.buffer), r);
					Profiling.totalRunLifeTime += (System.nanoTime() - r.getLifeTimeBegin());
					this.toDeleteRuns.add(r);
				}
			}
		
	}
	/**
	 * Creates a new run containing the input event.
	 * @param e The current event.
	 * @throws EvaluationException
	 */
	
	public void createNewRun(Event e) throws EvaluationException{
		if(this.nfa.getStates()[0].canStartWithEvent(e)){
			this.buffer.bufferEvent(e);
			Run newRun = this.engineRunController.getRun();
			newRun.initializeRun(this.nfa);
			newRun.addEvent(e);
			//this.numberOfRuns.update(1);
			Profiling.numberOfRuns ++;
			this.activeRuns.add(newRun);
		}
	}

	public void createNewRun(Event e, stats.Profiling profiling) throws EvaluationException{
		if(this.nfa.getStates()[0].canStartWithEvent(e)){
			this.buffer.bufferEvent(e);
			Run newRun = this.engineRunController.getRun();
			newRun.initializeRun(this.nfa);
			newRun.addEvent(e);
			//this.numberOfRuns.update(1);
			Profiling.numberOfRuns ++;
			this.activeRuns.add(newRun);
		}
	}
	/**
	 * Creates a new run containing the input event and adds the new run to the corresponding partition
	 * @param e The current event
	 * @throws EvaluationException
	 * @throws CloneNotSupportedException
	 */
	public void createNewRunByPartition(Event e) throws EvaluationException, CloneNotSupportedException{
		if(this.nfa.getStates()[0].canStartWithEvent(e)){
			this.buffer.bufferEvent(e);
			Run newRun = this.engineRunController.getRun();
			newRun.initializeRun(this.nfa);
			newRun.addEvent(e);
			//this.numberOfRuns.update(1);
			Profiling.numberOfRuns ++;
			this.activeRuns.add(newRun);
			this.addRunByPartition(newRun);

		}
	}
	/**
	 * Checks the predicate for e against r
	 * @param e The current event
	 * @param r The run against which e is evaluated 
	 * @return The check result, 0 for false, 1 for take or begin, 2 for proceed
	 */
	public int checkPredicateOptimized(Event e, Run r){//0 for false, 1 for take or begin, 2 for proceed
		int currentState = r.getCurrentState();
		State s = this.nfa.getStates(currentState);
		if(!s.getEventType().equalsIgnoreCase(e.getEventType())){// event type check;
			return 0;
		}

		if(!s.isKleeneClosure()){
			Edge beginEdge = s.getEdges(0);
			boolean result;
			//result = firstEdge.evaluatePredicate(e, r, buffer);
			result = beginEdge.evaluatePredicate(e, r, buffer);//
			if(result){
				return 1;
			}
		}else{
			if(r.isKleeneClosureInitialized()){
				boolean result;
				result = this.checkProceedOptimized(e, r);//proceedEdge.evaluatePredicate(e, r, buffer);
				if(result){
					return 2;
				}else{
				
				
				
				
				Edge takeEdge = s.getEdges(1);
				result = takeEdge.evaluatePredicate(e, r, buffer);
				if(result){
					return 1;
				}
				}
			}else{
				Edge beginEdge = s.getEdges(0);
				boolean result;
				
				result = beginEdge.evaluatePredicate(e, r, buffer);//
				if(result){
					return 1;
				}
			}
		}
		

		
		return 0;	
		
		
	}
	/**
	 * Checks whether the run needs to proceed if we add e to r
	 * @param e The current event
	 * @param r The run against which e is evaluated
	 * @return The checking result, TRUE for OK to proceed
	 */
	public boolean checkProceedOptimized(Event e, Run r){
		int currentState = r.getCurrentState();
		State s = this.nfa.getStates(currentState + 1);
		if(!s.getEventType().equalsIgnoreCase(e.getEventType())){// event type check;
			return false;
		}
		Edge beginEdge = s.getEdges(0);
		boolean result;
		result = beginEdge.evaluatePredicate(e, r, buffer);
		return result;
	}
/**
 * Checks whether the event satisfies the predicates of a run
 * @param e the current event
 * @param r the current run
 * @return the check result
 */
	
	public boolean checkPredicate(Event e, Run r){
		int currentState = r.getCurrentState();
		State s = this.nfa.getStates(currentState);
		if(!s.getEventType().equalsIgnoreCase(e.getEventType())){// event type check;
			return false;
		}

		if(!s.isKleeneClosure()){
			Edge beginEdge = s.getEdges(0);
			boolean result;
			//result = firstEdge.evaluatePredicate(e, r, buffer);
			result = beginEdge.evaluatePredicate(e, r, buffer);//
			if(result){
				return true;
			}
		}else{
			if(r.isKleeneClosureInitialized()){
				Edge takeEdge = s.getEdges(1);
				boolean result;
				result = takeEdge.evaluatePredicate(e, r, buffer);
				if(result){
					return true;
				}
			}else{
				Edge beginEdge = s.getEdges(0);
				boolean result;
				
				result = beginEdge.evaluatePredicate(e, r, buffer);//
				if(result){
					return true;
				}
			}
		}
		

		
		return false;	
		
		
	}
	/**
	 * Checks whether the event satisfies the partition of a run, only used under partition-contiguity selection strategy
	 * @param e the current event
	 * @param r the current run
	 * @return the check result, boolean format
	 */
	public boolean checkPartition(Event e, Run r){
		
		if(r.getPartitonId() == e.getAttributeByName(this.nfa.getPartitionAttribute())){
			return true;
		}
		return false;
	}
	
	/**
	 * Checks whether a kleene closure state can proceed to the next state
	 * @param r the current run
	 * @return the check result, boolean format
	 */
	
	
	public boolean checkProceed(Run r){// cannot use previous, only use position?
		int currentState = r.getCurrentState();
			
		Event previousEvent = this.buffer.getEvent(r.getPreviousEventId());
		State s = this.nfa.getStates(currentState);


		
		Edge proceedEdge = s.getEdges(2);
		boolean result;
		result = proceedEdge.evaluatePredicate(previousEvent, r, buffer);//
		if(result){
			return true;
		}
		
		return false;	
		
	}
	
	public boolean checkNegation(Event e) throws EvaluationException{
		if(this.nfa.getNegationState().canStartWithEvent(e)){
			return true;
		}
		return false;
	}

	public void indexNegationByPartition(Event e){
		int id = e.getAttributeByName(this.nfa.getPartitionAttribute());
		if(this.negationEventsByPartition.containsKey(id)){
			this.negationEventsByPartition.get(id).add(e);
		}else{
			ArrayList<Event> newPartition = new ArrayList<Event>();
			newPartition.add(e);
			this.negationEventsByPartition.put(id, newPartition);
		}
		
	}
	public boolean searchNegation(int beforeTimestamp, int afterTimestamp, ArrayList<Event> list){
		// basic idea is to use binary search on the timestamp
		int size = list.size();
		int lower = 0;
		int upper = size - 1;
		Event tempE;
		while(lower <= upper){
			tempE = list.get((lower + upper)/2);
			if(tempE.getTimestamp() >= beforeTimestamp && tempE.getTimestamp() <= afterTimestamp){
				return true;
			}
			if(tempE.getTimestamp() < beforeTimestamp){
				lower = (lower + upper)/2;
			}else {
				upper = (lower + upper)/2;
			}
			
		}
		return false;
	}
	public boolean searchNegationByPartition(int beforeTimestamp, int afterTimestamp, int partitionId){
		if(this.negationEventsByPartition.containsKey(partitionId)){
			ArrayList<Event> tempList = this.negationEventsByPartition.get(partitionId);
			return this.searchNegation(beforeTimestamp, afterTimestamp, tempList);
			
		}
		return false;
	}
	
/**
 * Clones a run
 * @param r the run to be cloned
 * @return the new run cloned from the input run.
 * @throws CloneNotSupportedException
 */
	
	public Run cloneRun(Run r) throws CloneNotSupportedException{
		Run newRun = this.engineRunController.getRun();
		newRun = (Run)r.clone();
		Profiling.numberOfRuns ++;
		return newRun;
	}
	
/**
 * Checks whether the event satisfies the time window constraint of a run
 * @param e the current event
 * @param r the current run
 * @return the check result
 */

	public boolean checkTimeWindow(Event e, Run r){
		if((e.getTimestamp() - r.getStartTimeStamp()) <= this.nfa.getTimeWindow()){
			return true;
		}
		return false;
	}

	

/**
 * Outputs a match, and profiles.	
 * @param m the match to be output
 */

public void outputMatch(sase.sasesystem.engine.Match m, long latency) {

	Profiling.numberOfMatches ++;
	String newMatch = "----------Here is the No." + Profiling.numberOfMatches +" match----------\n";
	newMatch+=m.toString()+"\n";
	if(ConfigFlags.printResults){
		if(!ConfigFlags.outFile.equalsIgnoreCase("")){
			try {
				File myObj = new File(ConfigFlags.outFile);
				if (myObj.createNewFile()) {
					System.out.println("File created: " + myObj.getName());
				}
				try {
					Writer output ;
					output = new BufferedWriter(new FileWriter(ConfigFlags.outFile, true));
					output.append(newMatch);
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}
		}else {
			System.out.println(newMatch);
			System.out.println("Latency == "+latency);
		}
	}
}


	public void outputMatch(sase.sasesystem.engine.Match m) {

		Profiling.numberOfMatches ++;
		String newMatch = "----------Here is the No." + Profiling.numberOfMatches +" match----------\n";
		newMatch+=m.toString()+"\n";
		if(ConfigFlags.printResults){
			if(!ConfigFlags.outFile.equalsIgnoreCase("")){
				try {
					File myObj = new File(ConfigFlags.outFile);
					if (myObj.createNewFile()) {
						System.out.println("File created: " + myObj.getName());
					}
					try {
						Writer output ;
						output = new BufferedWriter(new FileWriter(ConfigFlags.outFile, true));
						output.append(newMatch);
						output.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (IOException e) {
					System.out.println("An error occurred.");
					e.printStackTrace();
				}
			}else {
				System.out.println(newMatch);
			}
		}
	}
	/**
	 * Outputs a match, and profiles, for queries with a negation componengt, without a partition attribute.	
	 * @param m the match to be output
	 */
	public void outputMatchForNegation(Match m, Run r){
		if(this.searchNegation(r.getBeforeNegationTimestamp(), r.getAfterNegationTimestamp(), this.negationEvents)){
			Profiling.negatedMatches ++;
			System.out.println("~~~~~~~~~~~~~~~~Here is a negated match~~~~~~~~~~~~~~~");
			System.out.println(m);
		}else{
		
		Profiling.numberOfMatches ++;
		//this.matches.addMatch(m);
		if(ConfigFlags.printResults){
		System.out.println("----------Here is the No." + Profiling.numberOfMatches +" match----------");
		if(Profiling.numberOfMatches == 878){
			System.out.println("debug");
		}
		System.out.println(m);
		}
		
		}
		
	}
	/**
	 * Outputs a match, and profiles, for queries with a negation componengt, with a partition attribute.	
	 * @param m the match to be output
	 */
	public void outputMatchByPartitionForNegation(Match m, Run r){
		if(this.searchNegationByPartition(r.getBeforeNegationTimestamp(), r.getAfterNegationTimestamp(), r.getPartitonId())){
			Profiling.negatedMatches ++;
			System.out.println("~~~~~~~~~~~~~~~~Here is a negated match~~~~~~~~~~~~~~~");
			System.out.println(m);
		}else{
		
		Profiling.numberOfMatches ++;
		//this.matches.addMatch(m);
		if(ConfigFlags.printResults){
		System.out.println("----------Here is the No." + Profiling.numberOfMatches +" match----------");
		if(Profiling.numberOfMatches == 878){
			System.out.println("debug");
		}
		System.out.println(m);
		}
		
		}
		
	}
/**
 * Deletes runs violating the time window
 * @param currentTime current time
 * @param timeWindow time window of the query
 * @param delayTime specified delay period, any run which has been past the time window by this value would be deleted.
 */

	public void deleteRunsOverTimeWindow(int currentTime, int timeWindow, int delayTime){
		int size = this.activeRuns.size();
		Run tempRun = null;
		for(int i = 0; i < size; i ++){
			tempRun = this.activeRuns.get(i);
			if(!tempRun.isFull&&tempRun.getStartTimeStamp() + timeWindow + delayTime < currentTime){
				this.toDeleteRuns.add(tempRun);
				Profiling.numberOfRunsOverTimeWindow ++;
				
			}
		}
	}
	
	/**
	 * Cleans useless runs
	 */
	public void cleanRuns(){

		int size = this.toDeleteRuns.size();
		Run tempRun = null;
		for(int i = 0; i < size; i ++){
			tempRun = toDeleteRuns.get(0);
			Profiling.totalRunLifeTime += (System.nanoTime() - tempRun.getLifeTimeBegin());
			tempRun.resetRun();
			this.activeRuns.remove(tempRun);
			this.toDeleteRuns.remove(0);
			Profiling.numberOfRunsCutted ++;
		}
		

	}
	
	/**
	 * Cleans useless runs by partition.
	 */
	public void cleanRunsByPartition(){

		int size = this.toDeleteRuns.size();
		Run tempRun = null;
		ArrayList<Run> partitionedRuns = null;
		for(int i = 0; i < size; i ++){
			tempRun = toDeleteRuns.get(0);
			Profiling.totalRunLifeTime += (System.nanoTime() - tempRun.getLifeTimeBegin());
			tempRun.resetRun();
			this.activeRuns.remove(tempRun);
			this.toDeleteRuns.remove(0);
			Profiling.numberOfRunsCutted ++;
			partitionedRuns = this.activeRunsByPartition.get(tempRun.getPartitonId());
			partitionedRuns.remove(tempRun);
			if(partitionedRuns.size() == 0){
				this.activeRunsByPartition.remove(partitionedRuns);
			}
			
		}
		

	}


	/**
	 * @return the input
	 */
	public Stream getInput() {
		return input;
	}

	/**
	 * @param input the input to set
	 */
	public void setInput(Stream input) {
		this.input = input;
	}

	/**
	 * @return the buffer
	 */
	public EventBuffer getBuffer() {
		return buffer;
	}

	/**
	 * @param buffer the buffer to set
	 */
	public void setBuffer(EventBuffer buffer) {
		this.buffer = buffer;
	}

	/**
	 * @return the nfa
	 */
	public NFA getNfa() {
		return nfa;
	}

	/**
	 * @param nfa the nfa to set
	 */
	public void setNfa(NFA nfa) {
		this.nfa = nfa;
	}

	/**
	 * @return the engineRunController
	 */
	public RunPool getEngineRunController() {
		return engineRunController;
	}

	/**
	 * @param engineRunController the engineRunController to set
	 */
	public void setEngineRunController(RunPool engineRunController) {
		this.engineRunController = engineRunController;
	}


	/**
	 * @return the activeRuns
	 */
	public ArrayList<Run> getActiveRuns() {
		return activeRuns;
	}

	/**
	 * @param activeRuns the activeRuns to set
	 */
	public void setActiveRuns(ArrayList<Run> activeRuns) {
		this.activeRuns = activeRuns;
	}

	/**
	 * @return the matches
	 */
	public MatchController getMatches() {
		return matches;
	}

	/**
	 * @param matches the matches to set
	 */
	public void setMatches(MatchController matches) {
		this.matches = matches;
	}


	/**
	 * @return the toDeleteRuns
	 */
	public ArrayList<Run> getToDeleteRuns() {
		return toDeleteRuns;
	}

	/**
	 * @param toDeleteRuns the toDeleteRuns to set
	 */
	public void setToDeleteRuns(ArrayList<Run> toDeleteRuns) {
		this.toDeleteRuns = toDeleteRuns;
	}
	/**
	 * @return the activeRunsByPartiton
	 */
	public HashMap<Integer, ArrayList<Run>> getActiveRunsByPartiton() {
		return activeRunsByPartition;
	}
	/**
	 * @param activeRunsByPartiton the activeRunsByPartiton to set
	 */
	public void setActiveRunsByPartiton(
			HashMap<Integer, ArrayList<Run>> activeRunsByPartiton) {
		this.activeRunsByPartition = activeRunsByPartiton;
	}
	/**
	 * @return the activeRunsByPartition
	 */
	public HashMap<Integer, ArrayList<Run>> getActiveRunsByPartition() {
		return activeRunsByPartition;
	}
	/**
	 * @param activeRunsByPartition the activeRunsByPartition to set
	 */
	public void setActiveRunsByPartition(
			HashMap<Integer, ArrayList<Run>> activeRunsByPartition) {
		this.activeRunsByPartition = activeRunsByPartition;
	}
	/**
	 * @return the negationEvents
	 */
	public ArrayList<Event> getNegationEvents() {
		return negationEvents;
	}
	/**
	 * @param negationEvents the negationEvents to set
	 */
	public void setNegationEvents(ArrayList<Event> negationEvents) {
		this.negationEvents = negationEvents;
	}
	/**
	 * @return the negationEventsByPartition
	 */
	public HashMap<Integer, ArrayList<Event>> getNegationEventsByPartition() {
		return negationEventsByPartition;
	}
	/**
	 * @param negationEventsByPartition the negationEventsByPartition to set
	 */
	public void setNegationEventsByPartition(
			HashMap<Integer, ArrayList<Event>> negationEventsByPartition) {
		this.negationEventsByPartition = negationEventsByPartition;
	}


	public void setTransitions(HashMap<String, Transition> trans) {
		this.transitions = trans;
		for(Transition t: trans.values()){
			attribute = t.getP().getPredicateDescription().replaceAll("[^a-zA-Z^_]","");
			preds_per_state.put(Double.valueOf(t.getP().getPredicateDescription().replaceAll("[^0-9]","")),t.getSrc().getTag());
		}
	}

	public HashMap<String, Transition> getTransitions() {
		return transitions;
	}

	public int getWindow() {
		return this.nfa.getTimeWindow();
	}

	public void setConfigs(Configs configs) {
		this.configs = configs;
	}
}

