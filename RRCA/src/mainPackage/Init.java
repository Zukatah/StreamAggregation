package mainPackage;

//Necessary for file read method
/*import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;*/


public class Init
{
	private static int coreCount = 8;
	public static ProcUnit[] processingUnits;
	private static int OUTPUT_FILE = 1; //OUTPUT_CONSOLE = 0
	public static boolean GROUPBY_DEACTIVATED = false, GROUPBY_ACTIVATED = true;
	
	
	public static void main (String[] args)
	{		
		int threshold = 1024;
		int windowSize = 1000;
		int windowAdvance = 1;
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
		Tuple.coreCount = coreCount;
		processingUnits = new ProcUnit[coreCount];
		initPus(OUTPUT_FILE, "sumFile3600and600groupBy.txt", threshold);
		addOperationToAllPus(aggregateOperation, windowSize, windowAdvance, GROUPBY_ACTIVATED); //used when sending tuples without file
		startPus();
		sendTuplesTimed(threshold, tupleSendDuration, numberOfGroups);
	}
	
	
	
	private static void initPus (int output, String fileName, int threshold)
	{
		for (Integer i = 0; i < coreCount; i++)
		{
			processingUnits[i] = new ProcUnit(coreCount, i, output, fileName, threshold);
		}
		for (Integer i = 0; i < coreCount; i++)
		{
			processingUnits[i].leftNeighbour = processingUnits[(i-1 + coreCount) % coreCount];
			processingUnits[i].rightNeighbour = processingUnits[(i+1) % coreCount];
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
	
	
	
	private static void sendTuplesTimed (long sendThreshold, long duration, int numberOfGroups)
	{
		long starttime = System.currentTimeMillis();
		long counter = 0;
		while(System.currentTimeMillis() - starttime < duration)
		{
			boolean sleep = false;
			for (int i = 0; i < coreCount; i++)
			{
				if (processingUnits[i].approxIncOwnTuplesSize.get() >= sendThreshold)
				{
					sleep = true;
				}
			}
			if (sleep)
			{
				try {Thread.sleep(2);}
				catch (InterruptedException e) {e.printStackTrace();}
				continue;
			}
			for (int i = 0; i < coreCount; i++)
			{
				processingUnits[i].approxIncOwnTuplesSize.incrementAndGet();
				processingUnits[i].incOwnTuples.add(new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * numberOfGroups)).toString(), System.nanoTime(), i));
				counter++;
			}
			
		}
		
		Tuple temp = new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * numberOfGroups)).toString(), System.nanoTime(), 0);
		temp.finalTuple = true;
		processingUnits[0].approxIncOwnTuplesSize.incrementAndGet();
		processingUnits[0].incOwnTuples.add(temp);
		
		for (int i = 0; i < coreCount; i++)
		{
			processingUnits[i].noMoreTuples = true;
		}
	}
	
}
