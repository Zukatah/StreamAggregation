package mainPackage;

//Necessary for file read method
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;


public class Init
{
	private static int coreCount = 8;
	private static ProcUnit[] processingUnits;
	private static int OUTPUT_FILE = 1; //OUTPUT_CONSOLE = 0
	private static boolean GROUPBY_DEACTIVATED = false, GROUPBY_ACTIVATED = true;
	
	
	public static void main (String[] args)
	{
		int threshold = 1024;
		int windowSize = 360000;
		int windowAdvance = 2400;
		int numberOfGroups = 10;
		int aggregateOperation = AggregatOperation.OP_SUM;
		int tupleSendDuration = 60000;
		if (args.length > 0) { coreCount = Integer.parseInt(args[0]); }
		if (args.length > 1) { threshold = Integer.parseInt(args[1]); }
		if (args.length > 2) { windowSize = Integer.parseInt(args[2]); }
		if (args.length > 3) { windowAdvance = Integer.parseInt(args[3]); }
		if (args.length > 4) { numberOfGroups = Integer.parseInt(args[4]); }
		if (args.length > 5) { aggregateOperation = Integer.parseInt(args[5]); }
		if (args.length > 6) { tupleSendDuration = Integer.parseInt(args[6]); }
		processingUnits = new ProcUnit[coreCount];
		initPus(OUTPUT_FILE, "sumFile3600and600groupBy.txt", threshold);
		addOperationToAllPus(aggregateOperation, windowSize, windowAdvance, GROUPBY_ACTIVATED); //used when sending tuples without file
		startPus();
		sendTuplesTimed(threshold, tupleSendDuration, numberOfGroups);
	}
	
	//Test threshold values and neighbour checks if too many tuples groupby active and substitute blocking with cconcurrent list
	
	private static void initPus (int output, String fileName, int threshold)
	{
		for (Integer i = 0; i < coreCount; i++)
		{
			processingUnits[i] = new ProcUnit(coreCount, i, output, fileName, threshold);
		}
		for (Integer i = 0; i < coreCount; i++)
		{
			if (i != 0)
			{
				processingUnits[i].leftNeighbour = processingUnits[i-1];
			}
			if (i < coreCount - 1)
			{
				processingUnits[i].rightNeighbour = processingUnits[i+1];
			}
		}
	}
	
	
	private static void startPus ()
	{
		for (Integer i = 0; i < processingUnits.length; i++)
		{
			processingUnits[i].start();
		}
	}
	
	
	private static void addOperationToAllPus (int operationType, long windowSize, long windowStep, boolean groupBy)
	{
		for (int i = 0; i < coreCount; i++)
		{
			processingUnits[i].operationList.add(new AggregatOperation(operationType, windowSize, windowStep, groupBy));
		}
	}
	
	
	private static void sendTuples (long sleepDuration, long sendAmount, long sendThreshold, long tupleLimit)
	{
		long counter = 0;
		while(true)
		{
			while (processingUnits[0].approxIncTuplesSize.get() < sendThreshold)
			{
				processingUnits[0].approxIncTuplesSize.incrementAndGet();
				//processingUnits[0].incTuples.add(new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString()));
				processingUnits[0].incTuples.add(new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString(), System.nanoTime()));
				counter++;
				if (counter == tupleLimit - 1)
				{
					//Tuple temp = new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString());
					Tuple temp = new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString(), System.nanoTime());
					temp.finalTuple = true;
					processingUnits[0].approxIncTuplesSize.incrementAndGet();
					processingUnits[0].incTuples.add(temp);
					return;
				}
			}
			while (processingUnits[0].approxIncTuplesSize.get() >= sendThreshold)
			{
				try {Thread.sleep(2);}
				catch (InterruptedException e) {e.printStackTrace();}
			}
		}
	}
	
	
	private static void sendTuplesTimed (long sendThreshold, long duration, int numberOfGroups)
	{
		long starttime = System.currentTimeMillis();
		long counter = 0;
		while(System.currentTimeMillis() - starttime < duration)
		{
			while (processingUnits[0].approxIncTuplesSize.get() < sendThreshold)
			{
				processingUnits[0].approxIncTuplesSize.incrementAndGet();
				//processingUnits[0].incTuples.add(new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString()));
				//processingUnits[0].incTuples.add(new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString(), System.nanoTime()));
				processingUnits[0].incTuples.add(new Tuple(counter, System.currentTimeMillis(), 1.0, ((Long)(long)(Math.random() * numberOfGroups)).toString(), System.nanoTime()));
				counter++;
			}
			while (processingUnits[0].approxIncTuplesSize.get() >= sendThreshold)
			{
				try {Thread.sleep(2);}
				catch (InterruptedException e) {e.printStackTrace();}
			}
		}
		//Tuple temp = new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * 10.0)).toString());
		Tuple temp = new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * numberOfGroups)).toString(), System.nanoTime());
		temp.finalTuple = true;
		processingUnits[0].approxIncTuplesSize.incrementAndGet();
		processingUnits[0].incTuples.add(temp);
	}
	
	
	//Currently not in use - at the moment mainly for comparison purposes to validate if results are correct
	private static void sendTuplesFromFile(String inputFile, String separator, int indexTData, int indexDData, int indexSData, boolean timestampInDateForm) //inputFile.txt and sorted_data.csv, space and comma, 0 and 3, 1 and 5, -1 and 0, false and true
	{
		try(BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
		    ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
		    String line = null, followingLine = null;
		    String[] values;
		    String groupByString;
		    Long timestamp;
		    int higherIndex = indexDData > indexSData ? indexDData : indexSData;
		    
		    boolean fileEnd = false;
		    
		    while (!fileEnd)
		    {
		    	while (processingUnits[0].approxIncTuplesSize.get() > 10000)
		    	{
		    		try {Thread.sleep(10);}
					catch (InterruptedException e) {e.printStackTrace();}
		    	}
		    	
		    	int counter = 0;
		    	while (counter < 10000)
		    	{
		    		line = followingLine;
		    		followingLine = br.readLine();
		    		if (line == null && followingLine == null) {fileEnd = true; break; }
		    		if (line == null) { continue; }
		    		values = line.split(separator);
		    		if (values.length <= higherIndex) { fileEnd = true; System.out.println("Wrong input format..."); break; }
		    		if (indexSData != -1) { groupByString = values[indexSData]; } else { groupByString = ""; }
		    		if (timestampInDateForm)
		    		{
		    			Calendar temp = new GregorianCalendar();
			    		temp.set(Integer.parseInt(values[indexTData].substring(0, 4)), Integer.parseInt(values[indexTData].substring(5, 7)), Integer.parseInt(values[indexTData].substring(8, 10)), Integer.parseInt(values[indexTData].substring(11, 13)), Integer.parseInt(values[indexTData].substring(14, 16)), Integer.parseInt(values[indexTData].substring(17, 19)));
		    			timestamp = temp.getTimeInMillis();
		    		}
		    		else
		    		{
		    			timestamp = Long.parseLong(values[indexTData]);
		    		}
		    		tupleList.add(new Tuple(timestamp, System.currentTimeMillis(), Double.parseDouble(values[indexDData]), groupByString));
		    		if (followingLine == null) { tupleList.get(tupleList.size()-1).finalTuple = true; }
		    		counter++;
		    	}
		    	processingUnits[0].approxIncTuplesSize.addAndGet(tupleList.size());
		    	processingUnits[0].incTuples.addAll(tupleList);
		    	tupleList.clear();
		    }
		    System.out.println("Completed reading file...");
		}
		catch (FileNotFoundException e) { e.printStackTrace(); }
		catch (IOException e) { e.printStackTrace(); }
	}
}
