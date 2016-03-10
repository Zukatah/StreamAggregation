package mainPackage;


public class Window4x1KxCALC extends Window
{
	double result = 0.0;															//(Intermediate) result
	
	
	public Window4x1KxCALC (long windowEnd, AggregatOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		double temp = 0.0;
		final double tupleData = curTuple.dData;
		
		for (int i = 1; i <= 1000; i++)
		{
			temp += tupleData;
			temp -= i;
			temp *= tupleData;
			temp /= tupleData;
		}
		
		result += temp;
	}
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}