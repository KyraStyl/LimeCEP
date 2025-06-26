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
package sase.sasesystem.UI;

import sase.parser.InputParamParser;
import net.sourceforge.jeval.EvaluationException;
import sase.sasesystem.engine.EngineController;
import sase.sasesystem.engine.Profiling;
import sase.sasesystem.stream.ParseStockStreamConfig;
import sase.sasesystem.stream.StockStreamConfig;
import sase.sasesystem.stream.StreamController;

import java.io.IOException;


/**
 * The interface
 * @author haopeng
 *
 */
public class CommandLineUI {
	/**
	 * The main entry to run the engine under command line
	 * 
	 * @param args the inputs 
	 * 0: the nfa file location 
	 * 1: the stream config file
	 * 2: print the results or not (1 for print, 0 for not print)
	 * 3: use sharing techniques or not, ("sharingengine" for use, nothing for not use)
	 */
	public static String nfaFileLocation;
	public static String streamConfigFile;
	public static String inputFile;
	public static String eventtype;

	public static void main(String args[]) throws CloneNotSupportedException, EvaluationException, IOException{

		nfaFileLocation = "test.query";
		streamConfigFile = "test.stream";
		inputFile = "test.stream";
		eventtype = "stock";

		for (String arg : args) {
			System.out.print(arg + " ");
		}

		try {
			InputParamParser.validateParams(args);
			InputParamParser.readParams();
		}catch (Exception e){
			e.printStackTrace();
			System.out.println(InputParamParser.getHelp());
			System.exit(100);
		}

		if(streamConfigFile!="test.stream")
			ParseStockStreamConfig.parseStockEventConfig(streamConfigFile);
		StreamController myStreamController;
		EngineController myEngineController = new EngineController();
		myEngineController.setNfa(nfaFileLocation);
		myEngineController.setEngine();

		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
		myEngineController.initializeEngine();
		System.gc();
		myStreamController = new StreamController(StockStreamConfig.streamSize, eventtype+"Event");
		myStreamController.readStream(inputFile,eventtype);
		myEngineController.setInput(myStreamController.getMyStream());
		//myStreamController.printStream();
		myEngineController.runEngine();
		Profiling.memoryUsed = runtime.totalMemory() - runtime.freeMemory();
		Profiling.printProfiling();
			
			
	}
}

