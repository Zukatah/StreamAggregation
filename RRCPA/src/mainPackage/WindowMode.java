package mainPackage;

import java.util.Map.Entry;
import java.util.TreeMap;

public class WindowMode extends Window
{
	long numberCount = 0;															//
	double result = 0.0;															//(Intermediate) result
	TreeMap<Double, Long> tupleTreeMap = new TreeMap<Double, Long>();				//Store number of appearances of tuple doubles
	
	
	public WindowMode (long windowEnd, AggregateOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		Long temp = tupleTreeMap.get(curTuple.dData);
		if (temp == null) { temp = 1L; }
		else { temp++; }
		tupleTreeMap.put(curTuple.dData, temp);
	}
	
	
	public void calc(Window pane)
	{
		WindowMode curPane = (WindowMode)pane;
		while(!curPane.tupleTreeMap.isEmpty())
		{
			Entry<Double, Long> curEntry = curPane.tupleTreeMap.pollFirstEntry();
			Long temp = tupleTreeMap.get(curEntry.getKey());
			if (temp == null) { temp = 1L; }
			else { temp++; }
			tupleTreeMap.put(curEntry.getKey(), temp);
		}
		
		
	}
	
	
	public void finishCalc()
	{
		numberCount = 0;														//Maybe other way as well like in median? Just add everything in array, sort and iterate through it
		while(!tupleTreeMap.isEmpty())
		{
			Entry<Double, Long> curEntry = tupleTreeMap.pollFirstEntry();
			if (numberCount < curEntry.getValue())
			{
				numberCount = curEntry.getValue();
				result = curEntry.getKey();
			}
		}
	}
	
	
	public String getResult ()
	{
		return String.valueOf(result + "(" + numberCount + "x)");
	}
}