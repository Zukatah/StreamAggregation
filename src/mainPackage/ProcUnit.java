package mainPackage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;



public class ProcUnit extends Thread
{
	private final int coreCount;
	private final int myRank;
	private final int inputTupleThreshold;
	private final int outputMethod;																				//Only on first PU
	private PrintWriter writer = null;																			//Only on first PU
	public ProcUnit leftNeighbour = null;																		//Not on first PU
	public ProcUnit rightNeighbour = null;																		//Not on last PU
	private long timestampLatestTuple = Long.MIN_VALUE;															//Not necessary on last PU
	private long timestampLatestResultSet = 0;																	//Only on last PU (might cause troubles when handling negative timestamps!)
	public TreeMap<Long, HashMap<String, Window>> windowMap = new TreeMap<Long, HashMap<String, Window>>();
	public ArrayList<AggregatOperation> operationList = new ArrayList<AggregatOperation>();
	public ConcurrentLinkedQueue<Tuple> incTuples = new ConcurrentLinkedQueue<Tuple>();
	public AtomicInteger approxIncTuplesSize = new AtomicInteger(0);
	public ConcurrentLinkedQueue<ResultSet> results = new ConcurrentLinkedQueue<ResultSet>(); 					//Not on last PU
	
	private long latencySum = 0;																				//Only first PU (benchmark purposes)
	private long latencyCount = 0;																				//Only first PU (benchmark purposes)
	private long timeNeeded = System.currentTimeMillis();														//Only first PU (benchmark purposes)
	private long tuplesProcessed = 0;																			//Only first PU (benchmark purposes)
	
	private int tuplesSeenLastSecond = 0;
	private int windowsProcessedLastSecond = 0;
	private int outputTuplesProcessedLastSecond = 0;
	private long latencyCausedLastSecond = 0;
	private long lastTimeMeasured = 0;
	private ArrayList<Integer> tuplesSeenList = new ArrayList<Integer>();
	private ArrayList<Integer> windowsProcessedList = new ArrayList<Integer>();
	private ArrayList<Integer> outputTuplesProcessedList = new ArrayList<Integer>();
	private ArrayList<Long> latencyCausedList = new ArrayList<Long>();											//Value saved in micro seconds. Average time it took to take a tuple and begin the calculation since tuple arrival.
	
	
	private long nanotimeTuples = 0;
	private long nanotimeTuplesTemp = 0;
	private long nanotimeCalculation = 0;
	private long nanotimeCalculationTemp = 0;
	private long nanotimeGetAndCopy = 0;
	private long nanotimeGetAndCopyTemp = 0;
	private long nanotimeWindowPrep = 0;
	private long nanotimeWindowPrepTemp = 0;
	private long nanotimeWindowCalc = 0;
	private long nanotimeWindowCalcTemp = 0;
	private long nanotimeHash = 0;
	private long nanotimeHashTemp = 0;
	private long nanotimeResultSetCreation = 0;
	private long nanotimeResultSetCreationTemp = 0;
	private long nanotimeResultSets = 0;
	private long nanotimeResultSetsTemp = 0;
	
	
	public ProcUnit (int coreCount, int myRank, int outputMethod, String outputFilename, int inputTupleThreshold)
	{
		this.coreCount = coreCount;
		this.myRank = myRank;
		this.inputTupleThreshold = inputTupleThreshold;
		this.outputMethod = outputMethod;
		
		if (myRank == 0)
		{
			try { writer = new PrintWriter(outputFilename, "UTF-8"); }
			catch (FileNotFoundException e) { e.printStackTrace(); }
			catch (UnsupportedEncodingException e) { e.printStackTrace(); }
		}
	}
	
	
	public void run()
	{
		Tuple curTuple = null;
		ResultSet curResultSet = null;
		lastTimeMeasured = System.currentTimeMillis();
		long nextTimeToMeasure = lastTimeMeasured + 1000;
		
		
		while (true)
		{
			if (System.currentTimeMillis() >= nextTimeToMeasure)
			{
				tuplesSeenList.add(tuplesSeenLastSecond);
				windowsProcessedList.add(windowsProcessedLastSecond);
				outputTuplesProcessedList.add(outputTuplesProcessedLastSecond);
				latencyCausedList.add((tuplesSeenLastSecond==0) ? 0 : latencyCausedLastSecond/tuplesSeenLastSecond/1000 );
				tuplesSeenLastSecond = 0;
				windowsProcessedLastSecond = 0;
				outputTuplesProcessedLastSecond = 0;
				latencyCausedLastSecond = 0;
				lastTimeMeasured = nextTimeToMeasure;
				nextTimeToMeasure += 1000;
			}
			
			
			
			//Process received tuples
			nanotimeTuplesTemp = System.nanoTime();
			if (!incTuples.isEmpty() && (myRank == coreCount - 1 || rightNeighbour.approxIncTuplesSize.get() < inputTupleThreshold))
			{
				
				
				
				//nanotimeGetAndCopyTemp = System.nanoTime();
				approxIncTuplesSize.decrementAndGet();
				curTuple = incTuples.poll();
				tuplesProcessed++;
				tuplesSeenLastSecond++;
				timestampLatestTuple = curTuple.time; 								//We need to know the timestamp of the latest tuple to make sure we have all information needed for incoming ResultSets (not any longer!).
				long tempTime = System.nanoTime();
				latencyCausedLastSecond += (tempTime - curTuple.lastTimeProcessed);
				curTuple.lastTimeProcessed = tempTime;
				
				
				if (myRank != coreCount - 1)										//Copy the tuple we are treating now to the right neighbor
				{
					rightNeighbour.approxIncTuplesSize.incrementAndGet();
					rightNeighbour.incTuples.add(curTuple);
				}
				//nanotimeGetAndCopy += System.nanoTime() - nanotimeGetAndCopyTemp;
				
				
				for (int opIndex = 0; opIndex < operationList.size(); opIndex++)	//Iterate through all Operations, that are being executed currently
				{
					//nanotimeWindowPrepTemp = System.nanoTime();
					long windowStep = operationList.get(opIndex).windowStep;		//Load step and size of current operation to avoid loading it several times
					long windowSize = operationList.get(opIndex).windowSize;
					String hashString;												//Hash string to store windows within the corresponding hash map of the tree map. Consists of hashCode of operation and the optional groupBy value.
					String groupByString = "";										//Save the optional groupByString to avoid accessing it several times later. Used for window creation and to determine responsible PU via hash.
					if (operationList.get(opIndex).groupBy)
					{
						hashString = ((Integer)operationList.get(opIndex).hashCode()).toString().concat(curTuple.sData);
						groupByString = curTuple.sData;
					}
					else
					{
						hashString = ((Integer)operationList.get(opIndex).hashCode()).toString();
					}
					int groupByHashCode = groupByString.hashCode();
					
					
					
					
					
					
					long tempLong = curTuple.time % windowStep;
					long firstWindowEnd = curTuple.time + windowStep - tempLong;
					long lastWindowEnd = curTuple.time + windowSize - tempLong;
					//nanotimeWindowPrep += System.nanoTime() - nanotimeWindowPrepTemp;
					
					
					
					
					for (long curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowStep)	//Iterate through all Windows the current tuple might contribute to
					{
						//Hashfunction to distribute Windows evenly to the PUs
						
						//nanotimeHashTemp = System.nanoTime();
						if ( (((Long)(curWindowEnd+operationList.get(opIndex).operationId+groupByHashCode)).toString().hashCode() % coreCount + coreCount) % coreCount == myRank)
						{
							//nanotimeHash += System.nanoTime() - nanotimeHashTemp;
							windowsProcessedLastSecond++;
							
							//nanotimeWindowCalcTemp = System.nanoTime();
							Window curWindow = null;
							HashMap<String, Window> curHashMap = windowMap.get(curWindowEnd);
							if (curHashMap != null)
							{
								curWindow = curHashMap.get(hashString);
								if (curWindow == null)
								{
									curWindow = Window.CreateWindow(curWindowEnd, operationList.get(opIndex), groupByString);
									curWindow.handledByCore = myRank;	//Only temporary for tests
									curHashMap.put(hashString, curWindow);
								}
							}
							else
							{
								curHashMap = new HashMap<String, Window>();
								windowMap.put(curWindowEnd, curHashMap);
								curWindow = Window.CreateWindow(curWindowEnd, operationList.get(opIndex), groupByString);
								curWindow.handledByCore = myRank;		//Only temporary for tests
					        	curHashMap.put(hashString, curWindow);
							}
							//nanotimeWindowCalc += System.nanoTime() - nanotimeWindowCalcTemp;
							
							
							//nanotimeCalculationTemp = System.nanoTime();
				        	curWindow.calc(curTuple);
				        	//nanotimeCalculation += System.nanoTime() - nanotimeCalculationTemp;
						}
						
					}
					
				}
				
				
				//nanotimeResultSetCreationTemp = System.nanoTime();
				if (myRank == coreCount - 1)										//Check on the last PU, if a new ResultSet has to be created.
				{
					checkIfToCreateNewResultSet(curTuple);
				}
				//nanotimeResultSetCreation += System.nanoTime() - nanotimeResultSetCreationTemp;
			}
			nanotimeTuples += System.nanoTime() - nanotimeTuplesTemp;
			
			
			
			
			
			
			
			//Process received result sets
			nanotimeResultSetsTemp = System.nanoTime();
			if (!results.isEmpty() && timestampLatestTuple >= results.peek().timeLimit)
			{
				outputTuplesProcessedLastSecond++;
				curResultSet = results.poll();
				curResultSet.AddOldWindows(windowMap);
				
				if (curResultSet.finalResultSet)
				{
					System.out.println("Tuple time: " + (nanotimeTuples/1000000) + ". Calculation time: " + (nanotimeCalculation/1000000) + 
							". Get and Copy time: " + (nanotimeGetAndCopy/1000000) + ". Window Preparation time: " + (nanotimeWindowPrep/1000000) + 
							". Window Calculation time: " + (nanotimeWindowCalc/1000000) + ". Hash time: " + (nanotimeHash/1000000) + 
							". RSC time: " + (nanotimeResultSetCreation/1000000) + ". Result Set time: " + (nanotimeResultSets/1000000) + ". ");
				}
				
				if (leftNeighbour != null)
				{
					leftNeighbour.results.add(curResultSet);
					if (curResultSet.finalResultSet)
					{
						printLocalPuBenchmarkToFile();
						return;
					}
				}
				else
				{
					printResults(curResultSet);
					if (curResultSet.finalResultSet)
					{
						printLocalPuBenchmarkToFile();
						printGlobalBenchmarkToFile();
						return;
					}
				}
			}
			nanotimeResultSets += System.nanoTime() - nanotimeResultSetsTemp;
		}
	}
	
	
	
	private void checkIfToCreateNewResultSet(Tuple curTuple)
	{
		boolean sendResultSet = false;
		for (int i = 0; i < operationList.size(); i++)
		{
			long c = (timestampLatestResultSet / operationList.get(i).windowStep) * operationList.get(i).windowStep;
			if (c + operationList.get(i).windowStep <= curTuple.time)
			{
				sendResultSet = true;
				break;
			}
		}
		if (sendResultSet || curTuple.finalTuple)
		{
			ResultSet newResultSet = new ResultSet(curTuple.time - 1, curTuple.realtime);
			if (curTuple.finalTuple)
			{
				newResultSet.finalResultSet = true;
			}
			results.add(newResultSet);
			timestampLatestResultSet = curTuple.time;
		}
	}
	
	
	
	private void printResults(ResultSet curResultSet)
	{
		if (outputMethod == 0)	//Output to console
		{
			if (!curResultSet.windowMap.isEmpty())
			{
				System.out.println("RS with TL " + curResultSet.timeLimit + ". Latency: " + (System.currentTimeMillis() - curResultSet.timestampTrigTuple) + "ms.");
				latencySum += System.currentTimeMillis() - curResultSet.timestampTrigTuple;
				latencyCount++;
			}
			else
			{
				System.out.println("A ResultSet is empty. This may only happen once: for the last ResultSet.");
			}
			while (!curResultSet.windowMap.isEmpty())
			{
				Entry<Long, LinkedList<Window>> currentWindowList = curResultSet.windowMap.pollFirstEntry();
				//System.out.println("Windows with TL " + currentWindowList.getKey() + ":");
				Window curWindow;
				ListIterator<Window> windowIterator = currentWindowList.getValue().listIterator();
				while (windowIterator.hasNext())
		        {
		            curWindow = windowIterator.next();
		            System.out.println("Window time limit: " + curWindow.windowEnd + 
		            		", Group by: " + curWindow.groupBy +  
		            		", Result: " + curWindow.getResult() + 
		            		", Op: " + AggregatOperation.operationName[curWindow.operation.operationId] + 
		            		", on Thread " + curWindow.handledByCore + ".");
		        }
				
			}
		}
		else					//Output to file
		{
			if (!curResultSet.windowMap.isEmpty())
			{
				latencySum += (System.currentTimeMillis() - curResultSet.timestampTrigTuple);
				latencyCount++;
			}
			while (!curResultSet.windowMap.isEmpty())
			{
				Entry<Long, LinkedList<Window>> currentWindowList = curResultSet.windowMap.pollFirstEntry();
				Window curWindow;
				ListIterator<Window> windowIterator = currentWindowList.getValue().listIterator();
				while (windowIterator.hasNext())
		        {
		            curWindow = windowIterator.next();
		            writer.println("Window time limit: " + curWindow.windowEnd + 
		            		", Group by: " + curWindow.groupBy  + 
		            		", Result: " + curWindow.getResult() + 
		            		", Op: " + AggregatOperation.operationName[curWindow.operation.operationId] + 
		            		", on Thread " + curWindow.handledByCore + 
		            		", Latency: " + (System.currentTimeMillis() - curResultSet.timestampTrigTuple) + "ms.");
		        }
			}
		}
	}
	
	
	
	private void printGlobalBenchmarkToFile()
	{
		try {
		    Files.write(Paths.get("benchmark.txt"),
		    		(((Long)(latencySum/latencyCount)).toString() + " " + ((Long)(System.currentTimeMillis()-timeNeeded)).toString() + " " + ((Long)tuplesProcessed).toString() + "\n" ).getBytes(),
		    		StandardOpenOption.APPEND);
		    writer.flush();
		    writer.close();
		}
		catch (IOException e) { System.out.println("Error: " + e.getMessage()); }
	}
	
	
	
	private void printLocalPuBenchmarkToFile()
	{
		StringBuilder benchmarkString = new StringBuilder();
		for (int i = 0; i < tuplesSeenList.size(); i++)
		{
			benchmarkString.append(tuplesSeenList.get(i) + " " + windowsProcessedList.get(i) + " " + outputTuplesProcessedList.get(i) + " " + latencyCausedList.get(i) + "\n");
		}
		try
		{
			Files.write(Paths.get("PU" + String.valueOf(myRank) + "of" + String.valueOf(coreCount) + ".txt"),
					(benchmarkString.toString()).getBytes(),
					StandardOpenOption.CREATE);
		}
		catch (IOException e) { e.printStackTrace(); }
	}
}










//if ( (long)((double)curWindowStart * Math.PI + (double)(curWindowStart + windowSize) * Math.E) % coreCount == myRank) //groupBy.append(timeStamp).hashCode()
/*
writer1 = new BufferedWriter(new FileWriter (new File(outputFilename)));
writer1.write("Window TL: " + curWindow.windowEnd + ", Result: " + curWindow.result + ", Op: " + AggregatOperation.operationName[curWindow.operation.operationId] + ", auf Thread " + curWindow.handledByCore + ".");
writer1.newLine();

if (latencyCount % 250 == 0 && latencyCount > 0)
{
	System.out.println("Avg Latency: " + (latencySum/latencyCount) + "ms. Calc Time: " + (System.currentTimeMillis()-timeNeeded) + "ms. I1 " + Init.processingUnits[0].incTuples.size() + " T1 " + Init.processingUnits[0].tuples.size() + " I2 " + Init.processingUnits[1].incTuples.size() + " T2 " + Init.processingUnits[1].tuples.size() + " I3 " + Init.processingUnits[2].incTuples.size() + " T3 " + Init.processingUnits[2].tuples.size() + " I4 " + Init.processingUnits[3].incTuples.size()); //over last 250 windows
	latencySum=0;
	latencyCount=0;
	timeNeeded=System.currentTimeMillis();
}
*/