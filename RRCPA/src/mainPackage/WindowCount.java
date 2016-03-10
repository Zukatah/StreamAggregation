package mainPackage;


public class WindowCount extends Window
{
	long tupleCount = 0;
	
	
	public WindowCount (long windowEnd, AggregateOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		tupleCount++;
	}
	
	
	public void calc(Window pane)
	{
		tupleCount += ((WindowCount)pane).tupleCount;
	}
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(tupleCount);
	}
}