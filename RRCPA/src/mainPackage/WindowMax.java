package mainPackage;


public class WindowMax extends Window
{
	double result;															//(Intermediate) result
	
	
	public WindowMax (long windowEnd, AggregateOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
		result = Double.MIN_VALUE;
	}
	
	
	public void calc(Tuple curTuple)
	{
		if (curTuple.dData > result)
		{
			result = curTuple.dData;
		}
	}
	
	
	public void calc(Window pane)
	{
		if (((WindowMax)pane).result > result)
		{
			result = ((WindowMax)pane).result;
		}
	}
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}