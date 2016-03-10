package mainPackage;


public class WindowMin extends Window
{
	double result;															//(Intermediate) result
	
	
	public WindowMin (long windowEnd, AggregatOperation operation, String groupBy)
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
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}