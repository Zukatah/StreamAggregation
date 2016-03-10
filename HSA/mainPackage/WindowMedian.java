package mainPackage;

import java.util.ArrayList;

public class WindowMedian extends Window
{
	double result = 0.0;															//
	ArrayList<Double> tupleList = new ArrayList<Double>();							//Store tuple doubles
	//TreeMap<Double, Long> tupleTreeMap = new TreeMap<Double, Long>();				//Store number of appearances of tuple doubles
	
	
	public WindowMedian (long windowEnd, AggregatOperation operation, String groupBy)
	{
		super(windowEnd, operation, groupBy);
	}
	
	
	public void calc(Tuple curTuple)
	{
		tupleList.add(curTuple.dData);					//First possible way to go; less work at all but more in the end
		
		//Long temp = tupleTreeMap.get(curTuple.dData);	//Second way to go; more work but better distributed over time
		//if (temp == null) { temp = 1L; }
		//else { temp++; }
		//tupleTreeMap.put(curTuple.dData, temp);
	}
	
	
	public void finishCalc()
	{
		tupleList.sort(null);
		result = tupleList.get(tupleList.size()/2);
		
		/*long tupleCount = tupleTreeMap.size();								//Second way to go
		long tupleCountSoFar = 0;
		while(!tupleTreeMap.isEmpty())
		{
			Entry<Double, Long> curEntry = tupleTreeMap.pollFirstEntry();
			tupleCountSoFar += curEntry.getValue();
			if (tupleCountSoFar >= tupleCount/2)
			{
				result = curEntry.getKey();
				break;
			}
		}*/
	}
	
	
	public String getResult ()
	{
		return String.valueOf(result);
	}
}