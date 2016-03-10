package mainPackage;


public class WindowMin extends Window
{
	double result;															//(Intermediate) result
	
	
	public WindowMin (long windowEnd, AggregateOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
		result = Double.MAX_VALUE;
	}
	
	
	public void calc(Tuple curTuple)
	{
		if (curTuple.dData < result)
		{
			result = curTuple.dData;
		}
	}
	
	
	public void calc(Window pane)
	{
		if (((WindowMin)pane).result < result)
		{
			result = ((WindowMin)pane).result;
		}
	}
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}