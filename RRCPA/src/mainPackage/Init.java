package mainPackage;

import java.util.Random;

public class Init
{
	private static int coreCount = 1;
	public static ProcUnit[] processingUnits;
	private static int OUTPUT_FILE = 1; //OUTPUT_CONSOLE = 0
	public static boolean GROUPBY_DEACTIVATED = false, GROUPBY_ACTIVATED = true;
	public static AggregateOperation aggregateOperation;
	public static long tuplesInserted = 0;
	public static long sleepCount = 0;
	
	
	
	public static void main (String[] args)
	{		
		int threshold = 1024;
		int windowSize = 360000;
		int windowAdvance = 2400;
		int numberOfGroups = 10;
		int aggregateOperation = AggregateOperation.OP_SUM;
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
		initPus(OUTPUT_FILE, "sumFile3600and600groupBy.txt");
		addOperationToAllPus(aggregateOperation, windowSize, windowAdvance, GROUPBY_ACTIVATED); //used when sending tuples without file
		startPus();
		sendTuplesTimed(threshold, tupleSendDuration, numberOfGroups);
	}
	
	
	
	private static void initPus (int output, String fileName)
	{
		for (Integer i = 0; i < coreCount; i++)
		{
			processingUnits[i] = new ProcUnit(coreCount, i, output, fileName);
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
			processingUnits[i].aggregateOperation = new AggregateOperation(operationType, windowSize, windowStep, groupBy);
		}
		aggregateOperation = new AggregateOperation(operationType, windowSize, windowStep, groupBy);
	}
	
	
	
	private static void sendTuplesTimed (long sendThreshold, long duration, int numberOfGroups)
	{
		long starttime = System.currentTimeMillis();
		long counter = 0;
		Random random = new Random();
		
		while(System.currentTimeMillis() - starttime < duration)
		{			
			long paneInd = counter / aggregateOperation.windowStep;
			int responsiblePU = (int)(paneInd % coreCount);
			
			long remainingTuplesForPane = aggregateOperation.windowStep - (counter % aggregateOperation.windowStep);
			long approxIncTuplesSize = processingUnits[responsiblePU].approxIncTuplesSize.get();
			
			if (approxIncTuplesSize >= sendThreshold)
			{
				try {Thread.sleep(1); sleepCount++;}
				catch (InterruptedException e) {e.printStackTrace();}
				continue;
			}
			
			long remainingTuplesUntilThreshold = sendThreshold - approxIncTuplesSize;
			long tuplesToSend = (remainingTuplesUntilThreshold < remainingTuplesForPane) ? remainingTuplesUntilThreshold : remainingTuplesForPane;
			
			for (int i=0; i < tuplesToSend; i++)
			{
				Tuple newTuple = new Tuple(counter, System.currentTimeMillis(), 13.37 /*eigtl Math.random() aber zu aufwendig*/, ((Integer)random.nextInt(numberOfGroups)).toString(), paneInd);
				tuplesInserted++;
				processingUnits[responsiblePU].approxIncTuplesSize.incrementAndGet();
				processingUnits[responsiblePU].incTuples.add(newTuple);
				counter++;
			}
		}
		
		
		long paneInd = counter / aggregateOperation.windowStep; //long paneMod = counter % operationList.get(i).windowStep;
		int responsiblePU = (int)(paneInd % coreCount);
		Tuple newTuple = new Tuple(counter, System.currentTimeMillis(), Math.random(), ((Long)(long)(Math.random() * numberOfGroups)).toString(), paneInd);
		tuplesInserted++;
		processingUnits[responsiblePU].approxIncTuplesSize.incrementAndGet();
		processingUnits[responsiblePU].incTuples.add(newTuple);
		
		for (int i = 0; i < coreCount; i++)
		{
			processingUnits[i].noMoreTuples = true;
		}
	}
	
}
