package mainPackage;


public abstract class Window //make window abstract? 
{
	public int handledByCore = -1; 														//Just debug purposes
	public long windowEnd;																//Last timestamp, that may contribute to this window
	public AggregateOperation operation;													//Operation of window
	public String groupBy;																//Group-By string of window
	
	public long paneInd;
	public int paneLastPuToVisit;
	public long realtime;
	public boolean finalPane;
	
	
	public Window (long windowEnd, AggregateOperation operation, String groupBy)
	{
		this.windowEnd = windowEnd;
		this.operation = operation;
		this.groupBy = groupBy;
	}
	
	
	public abstract String getResult();
	public abstract void calc(Tuple curTuple);
	public abstract void calc(Window curPane);
	public abstract void finishCalc();
	
	
	public static Window CreateWindow (long windowEnd, AggregateOperation operation, String groupBy)
	{
		switch (operation.operationId)
		{
		case AggregateOperation.OP_AVERAGE:
			return new WindowAverage(windowEnd, operation, groupBy);
		case AggregateOperation.OP_COUNT:
			return new WindowCount(windowEnd, operation, groupBy);
		case AggregateOperation.OP_MAX:
			return new WindowMax(windowEnd, operation, groupBy);
		case AggregateOperation.OP_MEDIAN:
			return new WindowMedian(windowEnd, operation, groupBy);
		case AggregateOperation.OP_MIN:
			return new WindowMin(windowEnd, operation, groupBy);
		case AggregateOperation.OP_MODE:
			return new WindowMode(windowEnd, operation, groupBy);
		case AggregateOperation.OP_SUM:
			return new WindowSum(windowEnd, operation, groupBy);
		case AggregateOperation.OP_4x1KxCALC:
			return new Window4x1KxCALC(windowEnd, operation, groupBy);
		default:
			return null;
		}
	}
}



















/*
 * 
 * 
 * 	result += tuples.get(tuples.size()-1).dData; //Tuple bei manchen Ops schon direkt rauslöschen
 * 
 * 
 */
