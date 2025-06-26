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
package sase.sasesystem.stream;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * This class wraps the stream, specifies how to generate or import stream.
 * @author haopeng
 *
 */
public class StreamController {
	/**
	 * The stream
	 */
	Stream myStream;
	/**
	 * The size of the stream
	 */
	int size;
	
	/**
	 * event id
	 */
	int eventID;
	
	/**
	 * A random number generator
	 */
	Random randomGenerator;
	
	/**
	 * Default constructor
	 */
	public StreamController(){
		eventID = 0;
		randomGenerator = new Random(11);
	}
	
	/**
	 * Constructor, specified size and event type
	 * @param size
	 * @param eventType
	 */
	public StreamController(int size, String eventType){
		this.size = size;
		myStream = new Stream(size);
		if(eventType.equalsIgnoreCase("abcevent")){
			this.generateABCEvents();
		}
		if(eventType.equalsIgnoreCase("stockevent")){
			this.generateStockEvents();
		}
		if(eventType.equalsIgnoreCase("checkevent")){
			System.out.println("this is new to me!");
		}
		if(eventType.equalsIgnoreCase("realstockevent")){
			System.out.println("trying for a real dataset!");
		}
	}
	/**
	 * Generates a series of stock events
	 */
	public void generateStockEventsAsConfig(){
		Random r = new Random(StockStreamConfig.randomSeed);
		StockEvent events[] = new StockEvent[this.size];
		int id;
		int timestamp = 0;
		int symbol;
		int volume;
		int price = r.nextInt(100);
		int random = 0;
		String eventType = "stock";
		
		
		
		for (int i = 0; i < size; i ++){
			id = i;
			timestamp = id;
			
			symbol = r.nextInt(StockStreamConfig.numOfSymbol) + 1; 
			price = r.nextInt(StockStreamConfig.maxPrice) + 1;
			volume = r.nextInt(StockStreamConfig.maxVolume) + 1;
			
			events[i] = new StockEvent(id, timestamp, symbol, price, volume);				
		}
		myStream.setEvents(events);
		
		
	}
	
	/**
	 * Generates a series of stock events
	 */
	public void generateStockEventsAsConfigType(){
		if(StockStreamConfig.increaseProbability > 100){
		Random r = new Random(StockStreamConfig.randomSeed);
		StockEvent events[] = new StockEvent[this.size];
		int id;
		int timestamp = 0;
		int symbol;
		int volume;
		int price = r.nextInt(100);
		String eventType = "stock";
		
		
		
		for (int i = 0; i < size; i ++){
			id = i;
			timestamp = id;
			
			symbol = r.nextInt(StockStreamConfig.numOfSymbol) + 1; 
			price = r.nextInt(StockStreamConfig.maxPrice) + 1;
			volume = r.nextInt(StockStreamConfig.maxVolume) + 1;
			eventType = "stock" + symbol;
			events[i] = new StockEvent(id, timestamp, symbol, price, volume, eventType);				
		}
		myStream.setEvents(events);
		
		}else{
			this.generateStockEventsWithIncreaseProbability();
		}
	}
	
	/**
	 * Generates a series of stock events
	 */
	public void generateStockEventsWithIncreaseProbability(){
		
		Random r = new Random(StockStreamConfig.randomSeed);
		StockEvent events[] = new StockEvent[this.size];
		int id;
		int timestamp = 0;
		int symbol;
		int volume;
		int price[] = new int[StockStreamConfig.numOfSymbol];
		for(int i = 0; i < StockStreamConfig.numOfSymbol; i ++){
			//initializes the prices of each stock
			price[i] = r.nextInt(1000);
		}
		
		int random = 0;
		String eventType = "stock";
		
		
		
		for (int i = 0; i < size; i ++){
			id = i;
			timestamp = id;			
			symbol = r.nextInt(StockStreamConfig.numOfSymbol) + 1;
			random = r.nextInt(100) + 1;
			if(random <= StockStreamConfig.increaseProbability){
				//increase
				price[symbol - 1] += (r.nextInt(3) + 1);
			}else if(random > (100 + StockStreamConfig.increaseProbability) / 2){
				// decrease
				price[symbol - 1] -= (r.nextInt(3) + 1);				
			}
			
			
			volume = r.nextInt(StockStreamConfig.maxVolume) + 1;
			eventType = "stock";
			events[i] = new StockEvent(id, timestamp, symbol, price[symbol - 1], volume, eventType);				
		}
		myStream.setEvents(events);
		
		
	}
	
	/**
	 * Generates a series of stock events
	 */
//	public void generateStockEvents(){
//		Random r = new Random(11);
//		StockEvent events[] = new StockEvent[this.size];
//		int id;
//		int timestamp = 0;
//		int symbol;
//		int volume;
//		int price = r.nextInt(100);
//		int random = 0;
//		String eventType = "stock";
//
//		for (int i = 0; i < size; i ++){
//			id = i;
//			timestamp = id;
//			symbol = r.nextInt(2); //0 or 1
//			random = r.nextInt(100);
//			if(random < 55){
//				price += r.nextInt(5);
//			}else if(random >= 55 && random < 77){
//				price -= r.nextInt(5);
//			}
//			volume = r.nextInt(1000);
//
//			events[i] = new StockEvent(id, timestamp, symbol, price, volume);
//
//
//
//		}
//		myStream.setEvents(events);
//
//
//	}

	public void generateStockEvents(){
		Random r = new Random(11);
		StockEvent events[] = new StockEvent[this.size];
		int id;
		int timestamp = 0;
		int symbol = 0;
		int volume;
		int price = 1;
		int random = 0;
		String eventType = "stock";

		for (int i = 1; i <= size; i ++){
			id = i;
			timestamp = id;
			if(i<=3){
				symbol=0;
			}else if(i<=6){
				symbol=1;
			}else{
				symbol=2;
			}
			price = 1;
			volume = r.nextInt(1000);

			events[i-1] = new StockEvent(id, timestamp, symbol, price, volume);
		}
		myStream.setEvents(events);
	}
	
	/**
	 * Generates another batch of stock events
	 * @param number the size of the stream
	 */
	public void generateNextStockEvents(int number){
		
		StockEvent events[] = new StockEvent[number];
		int id;
		int timestamp = 0;
		int symbol;
		int volume;
		int price = this.randomGenerator.nextInt(100);
		int random = 0;
		String eventType = "stock";
		
		for (int i = 0; i < number; i ++){
			id = this.eventID;
			timestamp = id;
			symbol = this.randomGenerator.nextInt(2); //0 or 1
			random = this.randomGenerator.nextInt(100);
			if(random < 55){
				price += this.randomGenerator.nextInt(5);
			}else if(random >= 55 && random < 77){
				price -= this.randomGenerator.nextInt(5);
			}
			volume = this.randomGenerator.nextInt(1000);
			
			events[i] = new StockEvent(id, timestamp, symbol, price, volume);	
			this.eventID ++;
			
			
		}
		this.myStream = new Stream(number);
		myStream.setEvents(events);
	}
	
	/**
	 * Generates ABCEvents for the stream
	 */
	public void generateABCEvents(){
		// this is for correctness test
		Random r = new Random(11);
		ABCEvent events[] = new ABCEvent[this.size];
		int id;
		int timestamp = 0;
		int random = 0;
		String eventType = "";
		int price = 50;
		for (int i = 0; i < size; i ++){
			id = i;
			random = r.nextInt(3);
			timestamp = i;
			switch(random){
			case 0:
				eventType = "a";
				break;
			case 1:
				eventType = "b";
				price += 1;
				break;
			case 2:
				eventType = "c";
				price += 2;
				break;
			case 3:
				eventType = "d";
				price += 3;
				break;
			
			}
			
			events[i] = new ABCEvent(id, timestamp, eventType, price);
			
		}
		
		myStream.setEvents(events);
	}

	/**
	 * @return the myStream
	 */
	public Stream getMyStream() {
		return myStream;
	}

	/**
	 * @param myStream the myStream to set
	 */
	public void setMyStream(Stream myStream) {
		this.myStream = myStream;
		this.setSize(myStream.getSize());
	}

	/**
	 * @return the size
	 */
	public int getSize() {
		return size;
	}

	/**
	 * @param size the size to set
	 */
	public void setSize(int size) {
		this.size = size;
	}

	public void readStream(String filename, String eventtype){
		ArrayList<Event> events = new ArrayList<>();

		System.out.println("event type == "+eventtype);
		int id = 1;

		try{
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;
			while( (line = br.readLine()) !=null){
				if(eventtype.equalsIgnoreCase("Stock")){
					events.add(new StockEvent(line));
				}else if (eventtype.equalsIgnoreCase("Check")){
					events.add(new CheckEvent(line));
				}else if (eventtype.equalsIgnoreCase("realstock")) {
					events.add(new RealStock(line, id));
					id++;
				}else{
					System.out.println("Wrong event type!");
				}
			}

			if(eventtype.equalsIgnoreCase("Stock")){
				StockEvent[] ev = new StockEvent[events.size()];
				for(int i=0;i<events.size();i++){
					ev[i] = (StockEvent) events.get(i);
				}
				myStream.setEvents(ev);
			}else if (eventtype.equalsIgnoreCase("Check")){
				CheckEvent[] ev = new CheckEvent[events.size()];
				for(int i=0;i<events.size();i++){
					ev[i] = (CheckEvent) events.get(i);
				}
				myStream.setEvents(ev);
			} else if (eventtype.equalsIgnoreCase("RealStock")) {
				RealStock[] ev = new RealStock[events.size()];
				for(int i=0;i<events.size();i++){
					ev[i] = (RealStock) events.get(i);
				}
				myStream.setEvents(ev);
			} else{
				System.out.println("Wrong event type!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * Outputs the events in the stream one by one in the console
	 */
	public void printStream(){
		try {
			try {
				File myObj = new File("/home/kyrastyl/Desktop/master-thesis/my-sasesystem/eventStream.txt");
				if (myObj.createNewFile()) {
					System.out.println("File created: " + myObj.getName());
				} else {
					System.out.println("File already exists.");
				}
			} catch (IOException e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}
			FileWriter writer = new FileWriter("/home/kyrastyl/Desktop/master-thesis/my-sasesystem/eventStream.txt");
		  	for(int i = 0; i < myStream.getSize(); i ++){
				writer.write(myStream.getEvents()[i].toString());
				writer.write("\n");
			}
		  	writer.close();
		  	System.out.println("Successfully wrote to the file.");
		} catch (IOException e) {
		  	System.out.println("An error occurred.");
		  	e.printStackTrace();
		}

	}

	public void generateStockEvents2() {
		StockEvent[] events = new StockEvent[7];
		int symbol;
		for(int i=1;i<8;i++){
			if(i==1 || i==2 || i==4){
				symbol = 0;
			}else if(i==3 || i==5 || i==6){
				symbol = 1;
			}else{
				symbol = 2;
			}
			events[i-1] = new StockEvent(i,i,symbol,10,100);
		}
		this.myStream = new Stream(7);
		myStream.setEvents(events);
	}
}
