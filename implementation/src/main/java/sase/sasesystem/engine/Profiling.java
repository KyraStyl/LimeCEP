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

//
//import sase.hybridutils.CetManager;

import java.util.ArrayList;
import java.util.Locale;

/**
 * This class profiles the numbers of performance
 * @author haopeng
 *
 */
public class Profiling {
	
	/**
	 * The total running time of the engine (nanoseconds)
	 */
	public static long totalRunTime = 0;
	/**
	 * The number of events processed
	 */
	public static long numberOfEvents = 0;
	/**
	 * The number of runs generated
	 */
	public static long numberOfRuns = 0;
	/**
	 * The total run lifetime
	 */
	public static long totalRunLifeTime = 0;
	/**
	 * The number of matches
	 */
	public static long numberOfMatches = 0;
	/**
	 * The number of runs which ends before reach the final state
	 */
	public static long numberOfRunsCutted = 0;
	/**
	 * The number of runs which are deleted because they violate the time window constraint
	 */
	public static long numberOfRunsOverTimeWindow = 0;
	/**
	 * The cost on match construction
	 */
	public static long timeOfMatchConstruction = 0;
	
	public static long negatedMatches = 0;

	public static long memoryUsed = 0;

	public static long maxLatency = 0;
	public static long minLatency = Long.MAX_VALUE;
	public static long avgLatency = 0;

	public static int curWin = 0;

	public static ArrayList<Long> slideslatencyMax = new ArrayList<>();
	public static ArrayList<Long> slideslatencyAvg = new ArrayList<>();
	public static ArrayList<Long> slideslatencyMin = new ArrayList<>();
	public static ArrayList<Long> slideCets = new ArrayList<>();

	public static ArrayList<Long> slideSizes = new ArrayList<>();

	public static long tempStart = 0;
	public static long tempEnd = 0;
    public static int maxSizeMaximal = 0;

    /**
	 * resets the profiling numbers
	 */
	public static void resetProfiling(){
		
		 totalRunTime = 0;
		 numberOfEvents = 0;
		 numberOfRuns = 0;
		 totalRunLifeTime = 0;
		 numberOfMatches = 0;
		 numberOfRunsCutted = 0;
		 numberOfRunsOverTimeWindow = 0;
		 timeOfMatchConstruction = 0;
		 numberOfMergedRuns = 0;
		 negatedMatches = 0;
		 avgLatency = 0;
		 maxLatency = 0;
		 memoryUsed = 0;
		 tempStart = 0;
		 tempEnd = 0;
		 curWin = 0;
		 slideCets = new ArrayList<>();
		 slideslatencyMax = new ArrayList<>();
		 slideslatencyAvg = new ArrayList<>();
		 slideslatencyMin = new ArrayList<>();
	}

	public static void initSlide(){
		slideslatencyAvg.add(0L);
		slideslatencyMin.add(Long.MAX_VALUE);
		slideslatencyMax.add(0L);
		slideCets.add(0L);
	}

	public static void addCetNum() {
		long cur = 0;
		if(slideCets.size()>0)
			cur = slideCets.get(curWin);
		slideCets.set(curWin,cur+1);
		slideSizes.add(tempEnd-tempStart);
	}

	public static void updateLatency(long latency){
		if(latency> maxLatency)
			maxLatency = latency;
		else if(latency< minLatency)
			minLatency = latency;
		avgLatency += latency;
		if(ConfigFlags.engine.equalsIgnoreCase("cet"))
			updateLatencyForSlide(latency);
	}

	private static void updateLatencyForSlide(long latency){
		if(slideslatencyMax.size()>0){
			if(latency > slideslatencyMax.get(curWin)){
				slideslatencyMax.set(curWin,latency);
			}

		}else
			slideslatencyMax.set(curWin, latency);

		if(slideslatencyAvg.size()>0)
			slideslatencyAvg.set(curWin,latency+slideslatencyAvg.get(curWin));
		else
			slideslatencyAvg.set(curWin,latency);

		if(slideslatencyMin.size()>0){
			if(latency < slideslatencyMin.get(curWin)){
				slideslatencyMin.set(curWin,latency);
			}
		}else
			slideslatencyMin.set(curWin, latency);
	}


	/**
	 * prints the profiling numbers in console
	 */
	public static void printProfiling(){
		
		System.out.println();
		System.out.println("**************Profiling Numbers*****************");
		System.out.println("Engine used: " + ConfigFlags.engine.toUpperCase(Locale.ROOT));
		System.out.println("Total Running Time: " + ((double)totalRunTime  / 1_000_000_000)+" seconds");
		System.out.println("Number Of Events Processed: " + numberOfEvents);
		System.out.println("Number Of Runs Created: " + numberOfRuns);
		System.out.println("Number Of Matches Found: " + numberOfMatches);
		if(maxSizeMaximal > 0) System.out.println("Max Maximal Size: "+maxSizeMaximal);
		System.out.println("Used memory is bytes: " + memoryUsed);
		System.out.println("Used memory is megabytes: " + memoryUsed/(1024L*1024L));

		if(ConfigFlags.engine.equalsIgnoreCase("cet")){
			//System.out.print("Size of slides: ");
			//printList(slideSizes);
			System.out.print("Number of cets per slide: ");
			//printList(slideCets);
			System.out.print("Maximum Latency per slide in nano: ");
			//printList(slideslatencyMax);
			System.out.print("Minimum Latency per slide in nano: ");
			transfMin();
			//printList(slideslatencyMin);
			System.out.print("Average Latency per slide in nano: ");
			transfAvg();
			//printList(slideslatencyAvg);

		}
		System.out.println("Maximum Latency in nano: " + maxLatency);
		System.out.println("Minimum Latency in nano: " + minLatency);
//		if(ConfigFlags.engine.equalsIgnoreCase("cet"))
//			System.out.println("Average Latency in nano: " + avgLatency/CetManager.cetsdetectedtotal);
//		else
			System.out.println("Average Latency in nano: " + avgLatency/numberOfMatches);


		if(ConfigFlags.hasNegation){
			System.out.println("Number of Negated Matches: " + negatedMatches );
		}
	
		
		long throughput1 = 0;
		if(totalRunTime > 0){
			throughput1 = numberOfEvents* 1000000000/totalRunTime ;
			System.out.println("Throughput: " + throughput1 + " events/second");
		}
	}

	private static void transfMin() {
		for(int i=0;i<slideslatencyMin.size();i++){
			if(slideslatencyMin.get(i) == Long.MAX_VALUE)
				slideslatencyMin.set(i,0L);
		}
	}

	private static void printList(ArrayList<Long> list) {
		System.out.print("[");
		for (int i=0;i<list.size()-2;i++)
			System.out.print(list.get(i)+", ");
		System.out.println(list.get(list.size()-1)+"]");
	}

	private static void transfAvg() {
		for(int i=0;i<slideslatencyAvg.size();i++){
			long num = slideCets.get(i);
			long l = slideslatencyAvg.get(i);
			if( num !=0)
				slideslatencyAvg.set(i, l/num);
		}
	}


	// sharing numbers
	/**
	 * number of runs merged in the sharing engine
	 */
	public static long numberOfMergedRuns = 0;
	public static void resetSharingProfiling(){
		numberOfMergedRuns = 0;
	}
	/**
	 * outputs the extra profiling numbers for the sharing engine
	 */
	public static void printSharingProfiling(){
		System.out.println("#Merged Runs = " + numberOfMergedRuns);
	}

}
