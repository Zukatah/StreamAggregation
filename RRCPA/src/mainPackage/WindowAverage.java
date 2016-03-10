package mainPackage;


public class WindowAverage extends Window
{
	long tupleCount = 0;															//
	double result = 0.0;															//(Intermediate) result
	
	
	public WindowAverage (long windowEnd, AggregateOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		result += curTuple.dData;
		tupleCount++;
	}
	
	
	public void calc(Window pane)
	{
		result += ((WindowAverage)pane).result;
		tupleCount += ((WindowAverage)pane).tupleCount;
	}
	
	
	public void finishCalc()
	{
		if (tupleCount > 0) { result /= (double)tupleCount; }
	}
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}