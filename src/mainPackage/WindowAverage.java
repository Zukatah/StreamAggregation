package mainPackage;


public class WindowAverage extends Window
{
	long tupleCount = 0;															//
	double result = 0.0;															//(Intermediate) result
	
	
	public WindowAverage (long windowEnd, AggregatOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		result += curTuple.dData;
		tupleCount++;
		
		
		double temp2 = 0.0;
		double temp = curTuple.dData;
		for (int i = 1; i < 10000; i++)
		{
			temp = temp / i;
			temp2 += temp;
		}
		result += temp2;
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