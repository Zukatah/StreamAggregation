package mainPackage;


public class WindowSum extends Window
{
	double result = 0.0;															//(Intermediate) result
	
	
	public WindowSum (long windowEnd, AggregateOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		result += curTuple.dData;
	}
	
	
	public void calc(Window pane)
	{
		result += ((WindowSum)pane).result;
	}
	
	
	public void finishCalc() { }
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}