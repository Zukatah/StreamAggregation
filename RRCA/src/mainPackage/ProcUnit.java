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
	public boolean mayExit = false;
	public boolean noMoreTuples = false;
	
	private final int coreCount;
	private final int myRank;
	//private final int inputTupleThreshold;																	//Not in use; we try to synchronize only the incOwnTuples queues
	private final int outputMethod;																				//Only on first PU
	private PrintWriter writer = null;																			//Only on first PU
	public ProcUnit leftNeighbour = null;																		//Not on first PU
	public ProcUnit rightNeighbour = null;																		//Not on last PU
	private long timestampLatestTuple = Long.MIN_VALUE;															//Not necessary on last PU
	private long timestampLatestResultSet = 0;																	//Only on last PU (might cause troubles when handling negative timestamps!)
	public TreeMap<Long, HashMap<String, Window>> windowMap = new TreeMap<Long, HashMap<String, Window>>();
	public ArrayList<AggregatOperation> operationList = new ArrayList<AggregatOperation>();
	public ConcurrentLinkedQueue<Tuple> incTuples = new ConcurrentLinkedQueue<Tuple>();
	public ConcurrentLinkedQueue<Tuple> incOwnTuples = new ConcurrentLinkedQueue<Tuple>();
	public AtomicInteger approxIncTuplesSize = new AtomicInteger(0);
	public AtomicInteger approxIncOwnTuplesSize = new AtomicInteger(0);
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
	private long nanotimeResultSets = 0;
	private long nanotimeResultSetsTemp = 0;
	
	
	public ProcUnit (int coreCount, int myRank, int outputMethod, String outputFilename, int inputTupleThreshold)
	{
		this.coreCount = coreCount;
		this.myRank = myRank;
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
		Tuple curIncTuple = null;
		Tuple curOwnTuple = null;
		ResultSet curResultSet = null;
		lastTimeMeasured = System.currentTimeMillis();
		long nextTimeToMeasureLocalPUStats = lastTimeMeasured + 1000;
		
		
		while (true)
		{
			//Measure local PU stats
			if (System.currentTimeMillis() >= nextTimeToMeasureLocalPUStats)
			{
				nextTimeToMeasureLocalPUStats = measureLocalPUStats(nextTimeToMeasureLocalPUStats);
			}
			
			
			//Process received tuples
			nanotimeTuplesTemp = System.nanoTime();
			curIncTuple = incTuples.peek();
			curOwnTuple = incOwnTuples.peek();
			curTuple = null;
			if (curIncTuple != null || curOwnTuple != null)
			{
				if (curOwnTuple == null)
				{
					if (noMoreTuples)
					{
						curTuple = curIncTuple;
						approxIncTuplesSize.decrementAndGet();
						incTuples.remove();
					}
				}
				else
				{
					if (curIncTuple == null || curIncTuple.time >= curOwnTuple.time)
					{
						curTuple = curOwnTuple;
						approxIncOwnTuplesSize.decrementAndGet();
						incOwnTuples.remove();
					}
					else
					{
						curTuple = curIncTuple;
						approxIncTuplesSize.decrementAndGet();
						incTuples.remove();
					}
				}
				
				if (curTuple != null)
				{
					//----------Get&Copy part: Forward received tuples----------//
					//nanotimeGetAndCopyTemp = System.nanoTime();
					//if (myRank != curTuple.lastThreadEntered)
					//{
					//	while (rightNeighbour.approxIncTuplesSize.get() >= inputTupleThreshold) {}
					//}
					tuplesProcessed++;													//Count overall tuples processed
					tuplesSeenLastSecond++;												//Measuring local processing speed
					long tempTime = System.nanoTime();									//Measuring local latency
					latencyCausedLastSecond += (tempTime - curTuple.lastTimeProcessed);	//Measuring local latency
					curTuple.lastTimeProcessed = tempTime;								//Measuring local latency
					
					if (myRank != curTuple.lastThreadEntered)							//Copy the tuple we are treating now to the right neighbor, if we are not its last thread
					{
						rightNeighbour.approxIncTuplesSize.incrementAndGet();
						rightNeighbour.incTuples.add(curTuple);
					}
					else
					{
						timestampLatestTuple = curTuple.time; 							//We need to know the timestamp of the latest tuple to make sure we have all information needed for incoming ResultSets (not any longer?)
					}
					
					
					
					
					for (int opIndex = 0; opIndex < operationList.size(); opIndex++)				//Iterate through all Operations, that are being executed currently
					{
						//----------Store&TreatTupleInformation part: Calculate window boundaries for received tuple and a hash string----------//
						//nanotimeWindowPrepTemp = System.nanoTime();
						long windowStep = operationList.get(opIndex).windowStep;		//Load step and size of current operation to avoid loading it several times
						long windowSize = operationList.get(opIndex).windowSize;
						String hashString;												//Hash string to store windows within the corresponding hash map of the tree map. Consists of hashCode of operation and the optional groupBy value.
						String groupByString = "";										//Save the optional groupByString to avoid accessing it several times later. Used for window creation and to determine responsible PU via hash.
						if (operationList.get(opIndex).groupBy)
						{
							hashString = opIndex + " " + curTuple.sData;
							groupByString = curTuple.sData;
						}
						else
						{
							hashString = opIndex + "";									//Is there a better way?! Don't want to cast to Integer and then to String...
						}
						
						long localWindowStep = windowStep * coreCount;
						long localWindowOffset = myRank * windowStep;
						long tempLong = curTuple.time % localWindowStep;
						long firstWindowEnd = curTuple.time - tempLong + localWindowOffset;
						if (firstWindowEnd <= curTuple.time) { firstWindowEnd += localWindowStep; }
						long lastWindowEnd = curTuple.time + windowSize;
						//nanotimeWindowPrep += System.nanoTime() - nanotimeWindowPrepTemp;
						
						
						
						for (long curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += localWindowStep)	//Iterate through all Windows the current tuple might contribute to
						{
							//----------FindOrCreateWindow part: Find the necessary window or create it if not existent yet----------//
							//nanotimeWindowCalcTemp = System.nanoTime();
							windowsProcessedLastSecond++;
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
							
							
							//----------AggregateOperation part: Execute the calc method of the current window----------//
				        	curWindow.calc(curTuple);
						}
					}
					
					
					
					//nanotimeResultSetCreationTemp = System.nanoTime();
					if (myRank == coreCount - 1)										//Check on the last PU, if a new ResultSet has to be created.
					{
						checkIfToCreateNewResultSet(curTuple);
					}
					//nanotimeResultSetCreation += System.nanoTime() - nanotimeResultSetCreationTemp;
				}
			}
			nanotimeTuples += System.nanoTime() - nanotimeTuplesTemp;
			
			
			
			
			
			
			
			//Process received result sets
			nanotimeResultSetsTemp = System.nanoTime();
			curResultSet = results.peek();
			if (curResultSet != null && (timestampLatestTuple >= curResultSet.timeLimit || (noMoreTuples && incTuples.isEmpty() && incOwnTuples.isEmpty()) )) //previously: || curResultSet.finalResultSet
			{
				outputTuplesProcessedLastSecond++;
				results.remove();
				curResultSet.AddOldWindows(windowMap);
				
				if (curResultSet.finalResultSet)
				{
					System.out.println("Thread " + myRank + ". Tuple time: " + (nanotimeTuples/1000000) + ". Result Set time: " + (nanotimeResultSets/1000000) + ". ");
				}
				
				if (myRank != 0)
				{
					leftNeighbour.results.add(curResultSet);
					if (curResultSet.finalResultSet)
					{
						printLocalPuBenchmarkToFile();
					}
				}
				else
				{
					printAggregationResults(curResultSet);
					if (curResultSet.finalResultSet)
					{
						writer.flush();
						printLocalPuBenchmarkToFile();
						printGlobalLatTimeThroughputToFile();
						for (int i = 0; i < coreCount; i++)
						{
							Init.processingUnits[i].mayExit = true;
						}
						return;
					}
				}
			}
			nanotimeResultSets += System.nanoTime() - nanotimeResultSetsTemp;
			
			if (mayExit)
			{
				return;
			}
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
	
	
	
	private void printAggregationResults(ResultSet curResultSet)
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
		            curWindow.windowEnd = 1337; //Just to prevent the operations to be eliminated by optimization processes
		            /*writer.println("Window time limit: " + curWindow.windowEnd + 
		            		", Group by: " + curWindow.groupBy  + 
		            		", Result: " + curWindow.getResult() + 
		            		", Op: " + AggregatOperation.operationName[curWindow.operation.operationId] + 
		            		", on Thread " + curWindow.handledByCore + 
		            		", Latency: " + (System.currentTimeMillis() - curResultSet.timestampTrigTuple) + "ms.");*/
		        }
			}
		}
	}
	
	
	
	private void printGlobalLatTimeThroughputToFile()
	{
		try
		{
		    Files.write(Paths.get("benchmark.txt"),
		    		(((Long)(latencySum/latencyCount)).toString() + " " + ((Long)(System.currentTimeMillis()-timeNeeded)).toString() + " " + ((Long)tuplesProcessed).toString() + "\n" ).getBytes(),
		    		StandardOpenOption.APPEND);
		    writer.flush();
		    writer.close();
		}
		catch (IOException e) { System.out.println("Error: " + e.getMessage()); }
	}
	
	
	
	private long measureLocalPUStats(long nextTimeToMeasure)
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
		return nextTimeToMeasure;
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








/*
System.out.println("Thread " + myRank + ". Tuple time: " + (nanotimeTuples/1000000) + ". Calculation time: " + (nanotimeCalculation/1000000) + ". Get and Copy time: " + (nanotimeGetAndCopy/1000000) +
	". Window Preparation time: " + (nanotimeWindowPrep/1000000) + ". Window Calculation time: " + (nanotimeWindowCalc/1000000) + 
	". RSC time: " + (nanotimeResultSetCreation/1000000) + ". Result Set time: " + (nanotimeResultSets/1000000) + ". ");
	
	private long nanotimeCalculation = 0;
	//private long nanotimeCalculationTemp = 0;
	private long nanotimeGetAndCopy = 0;
	//private long nanotimeGetAndCopyTemp = 0;
	private long nanotimeWindowPrep = 0;
	//private long nanotimeWindowPrepTemp = 0;
	private long nanotimeWindowCalc = 0;
	//private long nanotimeWindowCalcTemp = 0;
	private long nanotimeResultSetCreation = 0;
	//private long nanotimeResultSetCreationTemp = 0;
*/