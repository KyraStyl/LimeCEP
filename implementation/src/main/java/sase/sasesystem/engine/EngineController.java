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

import events.ABCEvent;
import net.sourceforge.jeval.EvaluationException;
import sase.sasesystem.query.*;
import sase.sasesystem.stream.Stream;
import stats.Profiling;
import utils.Configs;
import managers.EventManager;
import managers.ResultManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

/**
 * This class is used to wrap the Engine class, 
 * such that when you write code, you can quickly locate the related methods.
 * @author haopeng
 *
 */
public class EngineController {
	/**
	 * The engine
	 */
	Engine myEngine;

	private Configs configs;
	private String engineType;

	/**
	 * Initializes the engine
	 */
	public void initializeEngine(){
		myEngine.initialize();
	}
	/**
	 * Default constructor.
	 */
	public EngineController(){
		myEngine = new Engine();
		engineType = "batch";
	}
	/**
	 * Constructor, can set different kinds of engines by different parameters
	 * @param engineType specifies the engine type, currently supports "sharingengine"
	 */
	public EngineController(String engineType){
		
			myEngine = new Engine();
			this.engineType = engineType;
		
	}
/**
 * Sets the nfa and selection strategy for the engine
 * @param selectionStrategy the selection strategy
 * @param nfaLocation the nfa file for the query
 */
	public void setNfa(String selectionStrategy, String nfaLocation){
		NFA nfa = new NFA(selectionStrategy, nfaLocation);
		HashMap<String, Transition> trans = calcTrans(nfa);
		myEngine.setNfa(nfa);
		myEngine.setTransitions(trans);
		configs.setTransitions(trans);
	}
/**
 * Sets the nfa for the engine.	
 * @param nfaLocation the nfa file for the query
 */
	public void setNfa(String nfaLocation){
		NFA nfa = new NFA(nfaLocation);
		HashMap<String, Transition> trans = calcTrans(nfa);
		myEngine.setNfa(nfa);
		myEngine.setTransitions(trans);
		if(this.configs!=null)
			configs.setTransitions(trans);
	}

	public HashMap<String, Transition> getTransitions(){
		return myEngine.getTransitions();
	}

	public int getWindow(){
		return myEngine.getWindow();
	}

	private HashMap<String, Transition> calcTrans(NFA nfa) {
		HashMap<String, Transition> hm = new HashMap<>();
		State states[] = nfa.getStates();
		for(int i = 0; i<states.length; i++){
			State s = states[i];
//			System.out.println("STATE == "+s);
//			System.out.println("STATE TYPE "+s.getEventType());
			Edge edges[] = s.getEdges();
			ArrayList<PredicateOptimized> preds = new ArrayList<>();
			for( Edge e: edges )
				for (PredicateOptimized p : e.getPredicates())
					preds.add(p);

			for( PredicateOptimized p : preds){
				if( p.getRightOperands().toString().contains("nonVar") ){
					State dst = null;
					if(i<states.length-1)
						dst = states[i+1];
					Transition t = new Transition(s, dst, s.getEventType(), p);
					if(this.configs!=null)
						this.configs.setKleeneState(s.getEventType(),s.isKleeneClosure());
					ConfigFlags.isKleene.put(s.getTag().replace("+",""),s.isKleeneClosure());
					hm.put(s.getTag(), t);
				}
			}
		}

		return hm;
	}

	public void setEngine(){
		NFA nfa = myEngine.getNfa();
		String engine = "sasesystem";
		if(nfa.containsKleene)
			if(nfa.getSelectionStrategy().equalsIgnoreCase("skip-till-any-match"))
				if(nfa.getTimeWindow() >= 100)
					engine = "cet";

		if(ConfigFlags.engine == "null")
			ConfigFlags.engine = engine;
	}
/**
 * Sets the input stream for the engine
 * @param input the input stream
 */
	public void setInput(Stream input){
		myEngine.setInput(input);
	}
/**
 * starts to run the engine	
 * @throws CloneNotSupportedException
 * @throws EvaluationException
 */
	public void runEngine() throws CloneNotSupportedException, EvaluationException{
		myEngine.runEngine();
	}

	public void runEngine(events.ABCEvent e, EventManager<events.ABCEvent> eventManager) throws CloneNotSupportedException, EvaluationException{
		if(engineType == "once")
			myEngine.runOnce(e, eventManager);
	}

	public void runOnDemand(HashMap<String, TreeSet<ABCEvent>> subsets, events.ABCEvent trigger_evt){
		ArrayList<ABCEvent> list = new ArrayList<>();
		list.add(trigger_evt);

		myEngine.find_matches_once(1,subsets,list,true);
	}

	public void runSASEonce(ABCEvent e, Profiling profiling){
        try {
            myEngine.runOnceSTAMnoPartition(e, profiling);
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        } catch (EvaluationException ex) {
            throw new RuntimeException(ex);
        }
    }

	public void setConfigs(Configs configs) {
		this.configs = configs;
		myEngine.setConfigs(configs);
	}

	public void setEngineResultManager(ResultManager resultManager){
		myEngine.setResultManager(resultManager);
	}

	public State[] getStates() {
		return this.myEngine.getNfa().getStates();
	}
}
