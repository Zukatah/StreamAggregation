package mainPackage;


public class WindowMax extends Window
{
	double result;															//(Intermediate) result
	
	
	public WindowMax (long windowEnd, AggregatOperation operation, String groupBy)
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
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}