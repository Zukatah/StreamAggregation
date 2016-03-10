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
	private final int outputMethod;																				//Only on first PU
	private PrintWriter writer = null;																			//Only on first PU
	public ProcUnit leftNeighbour = null;																		//Not on first PU
	public ProcUnit rightNeighbour = null;																		//Not on last PU
	private long timestampLatestPane = Long.MIN_VALUE;
	private long timestampLatestResultSet = 0;																	//Only on last PU (might cause troubles when handling negative timestamps!)
	public TreeMap<Long, HashMap<String, Window>> windowMap = new TreeMap<Long, HashMap<String, Window>>();
	public AggregateOperation aggregateOperation;
	public ConcurrentLinkedQueue<Tuple> incTuples = new ConcurrentLinkedQueue<Tuple>();
	public AtomicInteger approxIncTuplesSize = new AtomicInteger(0);
	public ConcurrentLinkedQueue<Window> incPanes = new ConcurrentLinkedQueue<Window>();
	public ConcurrentLinkedQueue<Window> finishedOwnPanes = new ConcurrentLinkedQueue<Window>();
	public ConcurrentLinkedQueue<ResultSet> results = new ConcurrentLinkedQueue<ResultSet>(); 					//Not on last PU
	
	private long latencySum = 0;																				//Only first PU (benchmark purposes)
	private long latencyCount = 0;																				//Only first PU (benchmark purposes)
	private long timeNeeded = System.currentTimeMillis();														//Only first PU (benchmark purposes)
	
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
	private long nanotimePanes = 0;
	private long nanotimePanesTemp = 0;
	private long nanotimeResultSets = 0;
	private long nanotimeResultSetsTemp = 0;
	
	
	
	public ProcUnit (int coreCount, int myRank, int outputMethod, String outputFilename)
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
		ResultSet curResultSet = null;
		HashMap<String, Window> activePanes = new HashMap<String, Window>();
		Window activePane = null;
		long activePanesTimeLimit = 0;
		Window curOwnPane = null;
		Window curRecPane = null;
		Window curPane = null;
		int paneTravelTarget;
		int expectedFinalPanes;
		lastTimeMeasured = System.currentTimeMillis();
		long nextTimeToMeasureLocalPUStats = lastTimeMeasured + 1000;
		
		if (aggregateOperation.windowSize / aggregateOperation.windowStep >= coreCount)
		{ 
			paneTravelTarget = (myRank + coreCount - 1) % coreCount;
			expectedFinalPanes = coreCount;
		}
		else
		{ 
			paneTravelTarget = (myRank + (int)(aggregateOperation.windowSize / aggregateOperation.windowStep) - 1) % coreCount;
			expectedFinalPanes = (int)(aggregateOperation.windowSize / aggregateOperation.windowStep);
		}
		
		
		while (true)
		{
			//Measure local PU stats
			if (System.currentTimeMillis() >= nextTimeToMeasureLocalPUStats)
			{
				nextTimeToMeasureLocalPUStats = measureLocalPUStats(nextTimeToMeasureLocalPUStats);
			}
			
			
			
			//----------Process received tuples----------//
			nanotimeTuplesTemp = System.nanoTime();
			curTuple = incTuples.peek();
			if (curTuple != null)
			{
				approxIncTuplesSize.decrementAndGet();
				incTuples.remove();
				
				tuplesSeenLastSecond++;												//Measuring local processing speed
				
				
				if (activePanes.isEmpty())
				{
					activePane = Window.CreateWindow(curTuple.paneIndex*aggregateOperation.windowStep+aggregateOperation.windowStep, aggregateOperation, curTuple.sData);
					activePane.paneInd = curTuple.paneIndex;
					activePane.paneLastPuToVisit = paneTravelTarget;
					activePane.realtime = curTuple.realtime;
					activePanes.put(curTuple.sData, activePane);
					activePanesTimeLimit = activePane.windowEnd;
				}
				else
				{
					if (activePanesTimeLimit <= curTuple.time)
					{
						finishedOwnPanes.addAll(activePanes.values());
						activePanes.clear();
						activePane = Window.CreateWindow(curTuple.paneIndex*aggregateOperation.windowStep+aggregateOperation.windowStep, aggregateOperation, curTuple.sData);
						activePane.paneInd = curTuple.paneIndex;
						activePane.paneLastPuToVisit = paneTravelTarget;
						activePane.realtime = curTuple.realtime;
						activePanes.put(curTuple.sData, activePane);
						activePanesTimeLimit = activePane.windowEnd;
					}
					else
					{
						if (activePanes.containsKey(curTuple.sData))
						{
							activePane = activePanes.get(curTuple.sData);
						}
						else
						{
							activePane = Window.CreateWindow(curTuple.paneIndex*aggregateOperation.windowStep+aggregateOperation.windowStep, aggregateOperation, curTuple.sData);
							activePane.paneInd = curTuple.paneIndex;
							activePane.paneLastPuToVisit = paneTravelTarget;
							activePane.realtime = curTuple.realtime;//
							activePanes.put(curTuple.sData, activePane);
						}
					}
				}
				activePane.calc(curTuple);
			}
			else
			{
				if (noMoreTuples)
				{
					if (!activePanes.isEmpty())
					{
						ArrayList<Window> activePanesList = new ArrayList<Window>(activePanes.values());
						activePanesList.get(activePanesList.size()-1).finalPane = true;
						finishedOwnPanes.addAll(activePanesList);
						activePanes.clear();
					}
				}
			}
			nanotimeTuples += System.nanoTime() - nanotimeTuplesTemp;
			
			
			
			//----------Process own finished panes and received ones----------//
			nanotimePanesTemp = System.nanoTime();
			do {
				curRecPane = incPanes.peek();
				curOwnPane = finishedOwnPanes.peek();
				curPane = null;
				if (curRecPane != null || curOwnPane != null)
				{
					if (curOwnPane == null)
					{
						if (noMoreTuples && activePanes.isEmpty() && incTuples.isEmpty())
						{
							curPane = curRecPane;
							incPanes.remove();
						}
					}
					else
					{
						if (curRecPane == null || curRecPane.windowEnd >= curOwnPane.windowEnd)
						{
							curPane = curOwnPane;
							finishedOwnPanes.remove();
						}
						else
						{
							curPane = curRecPane;
							incPanes.remove();
						}
					}
					
					if (curPane != null)
					{
						if (myRank == curPane.paneLastPuToVisit)
						{
							timestampLatestPane = curPane.windowEnd - 1; //There might come other panes with the same window end (and different group by values), so we may not calculate windows with this time-limit yet
						}
						else
						{
							rightNeighbour.incPanes.add(curPane);
						}
						if (curPane.finalPane)
						{
							expectedFinalPanes--;
							if (myRank == coreCount - 1 && expectedFinalPanes == 0)
							{
								ResultSet newResultSet = new ResultSet(curPane.windowEnd, curPane.realtime); //previously windowEnd-1; but should also work without -1
								newResultSet.finalResultSet = true;
								results.add(newResultSet);
								timestampLatestResultSet = curPane.windowEnd;
							}
						}
						
						long windowStep = aggregateOperation.windowStep;				//Load step and size of current operation to avoid loading it several times
						long windowSize = aggregateOperation.windowSize;
						String groupByString = "";										//Save the optional groupByString to avoid accessing it several times later. Used for window creation and to determine responsible PU via hash.
						if (aggregateOperation.groupBy)
						{
							groupByString = curPane.groupBy;
						}
						
						long localWindowStep = coreCount;
						long localWindowOffset = myRank;
						long firstWindowIndex = curPane.paneInd / coreCount * coreCount + localWindowOffset;
						if (firstWindowIndex < curPane.paneInd) { firstWindowIndex += localWindowStep; }
						long lastWindowIndex = curPane.paneInd + windowSize/windowStep - 1;
						
						for (long curWindowIndex = firstWindowIndex; curWindowIndex <= lastWindowIndex; curWindowIndex += localWindowStep)	//Iterate through all Windows the current tuple might contribute to
						{
							windowsProcessedLastSecond++;
							Window curWindow = null;
							long curWindowEnd = (curWindowIndex + 1) * windowStep;
							HashMap<String, Window> curHashMap = windowMap.get(curWindowEnd);
							if (curHashMap != null)
							{
								curWindow = curHashMap.get(groupByString);
								if (curWindow == null)
								{
									curWindow = Window.CreateWindow(curWindowEnd, aggregateOperation, groupByString);
									curWindow.handledByCore = myRank;	//Only temporary for tests
									curHashMap.put(groupByString, curWindow);
								}
							}
							else
							{
								curHashMap = new HashMap<String, Window>();
								windowMap.put(curWindowEnd, curHashMap);
								curWindow = Window.CreateWindow(curWindowEnd, aggregateOperation, groupByString);
								curWindow.handledByCore = myRank;		//Only temporary for tests
					        	curHashMap.put(groupByString, curWindow);
							}
							
				        	curWindow.calc(curPane);
						}
						
						if (myRank == coreCount - 1)										//Check on the last PU, if a new ResultSet has to be created.
						{
							checkIfToCreateNewResultSet(curPane);
						}
					}
				}
			} while (!finishedOwnPanes.isEmpty());
			nanotimePanes += System.nanoTime() - nanotimePanesTemp;
			
			
			
			//----------Process received result sets----------//
			nanotimeResultSetsTemp = System.nanoTime();
			curResultSet = results.peek();
			if (curResultSet != null && (timestampLatestPane >= curResultSet.timeLimit || expectedFinalPanes == 0))
				//|| (incTuples.isEmpty() && finishedOwnPanes.isEmpty() && incPanes.isEmpty() && noMoreTuples)
				//Was passiert wenns nicht das finale ResultSet ist?! Noch nicht alle Panes fertig => unfertige Windows
			{
				outputTuplesProcessedLastSecond++;
				results.remove();
				curResultSet.AddOldWindows(windowMap);
				
				if (curResultSet.finalResultSet)
				{
					System.out.println("Thread " + myRank + ". Tuple time: " + (nanotimeTuples/1000000) + ". Pane time: " + (nanotimePanes/1000000) + ". Result Set time: " + (nanotimeResultSets/1000000) + ".");
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
	
	
	
	private void checkIfToCreateNewResultSet(Window curPane)
	{
		boolean sendResultSet = false;
		long c = (timestampLatestResultSet / aggregateOperation.windowStep) * aggregateOperation.windowStep;
		if (c + aggregateOperation.windowStep <= curPane.windowEnd)
		{
			sendResultSet = true;
		}
		if (sendResultSet) //|| (noMoreTuples && incTuples.isEmpty() && finishedOwnPanes.isEmpty() && incPanes.isEmpty())
		{
			ResultSet newResultSet = new ResultSet(curPane.windowEnd, curPane.realtime); //previously windowEnd-1; but should also work without -1
			//if (noMoreTuples && incTuples.isEmpty() && finishedOwnPanes.isEmpty() && incPanes.isEmpty()) { newResultSet.finalResultSet = true; }
			results.add(newResultSet);
			timestampLatestResultSet = curPane.windowEnd;
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
		            		", Op: " + AggregateOperation.operationName[curWindow.operation.operationId] + 
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
		    		(((Long)(latencySum/latencyCount)).toString() + " " + ((Long)(System.currentTimeMillis()-timeNeeded)).toString() + " " + ((Long)Init.tuplesInserted).toString() + " " + Init.sleepCount +  "\n" ).getBytes(),
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
long localWindowStep = windowStep * coreCount;
long localWindowOffset = myRank * windowStep;
long tempLong = (curPane.windowEnd - 1) % localWindowStep;
long firstWindowEnd = curTuple.time - tempLong + localWindowOffset;
if (firstWindowEnd <= curTuple.time) { firstWindowEnd += localWindowStep; }
long lastWindowEnd = curTuple.time + windowSize;

curPane.finalPane && myRank == curPane.paneLastPuToVisit))

if (curResultSet != null && (timestampLatestPane >= curResultSet.timeLimit || curResultSet.finalResultSet )) //|| (incTuples.isEmpty() && incOwnTuples.isEmpty())

System.out.println("Thread " + myRank + ". Tuple time: " + (nanotimeTuples/1000000) + ". Calculation time: " + (nanotimeCalculation/1000000) + ". Get and Copy time: " + (nanotimeGetAndCopy/1000000) +
							". Window Preparation time: " + (nanotimeWindowPrep/1000000) + ". Window Calculation time: " + (nanotimeWindowCalc/1000000) + 
							". RSC time: " + (nanotimeResultSetCreation/1000000) + ". Result Set time: " + (nanotimeResultSets/1000000) + " sleepCount: " + Init.sleepCount + ".");

private long nanotimeTuples = 0;
	private long nanotimeTuplesTemp = 0;
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
	private long nanotimeResultSets = 0;
	private long nanotimeResultSetsTemp = 0;

//----------Get&Copy part: Forward received tuples----------//
//nanotimeGetAndCopyTemp = System.nanoTime();

//----------AggregateOperation part: Execute the calc method of the current window----------//

//----------Store&TreatTupleInformation part: Calculate window boundaries for received tuple and a hash string----------//
//nanotimeWindowPrepTemp = System.nanoTime();
//nanotimeWindowPrep += System.nanoTime() - nanotimeWindowPrepTemp;

//----------FindOrCreateWindow part: Find the necessary window or create it if not existent yet----------//
//nanotimeWindowCalcTemp = System.nanoTime();

//nanotimeResultSetCreationTemp = System.nanoTime();
//nanotimeResultSetCreation += System.nanoTime() - nanotimeResultSetCreationTemp;

//System.out.println("Thread " + myRank + ": ResultSet " + curResultSet.timeLimit + ".");

*/
