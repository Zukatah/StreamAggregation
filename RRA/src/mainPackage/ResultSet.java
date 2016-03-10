package mainPackage;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;


public class ResultSet
{
	public TreeMap<Long, LinkedList<Window>> windowMap = new TreeMap<Long, LinkedList<Window>>();		//Using a TreeMap to store and sort all LinkedLists (for storing Windows with the same expiration time) by their expiration time.
	long timeLimit;																						//This ResultSet stores all available windows of all PUs up to this timeLimit.
	long timestampTrigTuple;																			//We need to know the realtime timestamp of the tuple triggering this ResultSet to calculate the latency of the ouput.
	boolean finalResultSet = false;																		//Marks the final ResultSet to be able to flush and close file writers.
	
	
	public ResultSet (long timeLimit, long timestampTrigTuple)
	{
		this.timeLimit = timeLimit;
		this.timestampTrigTuple = timestampTrigTuple;
	}
	
	
	public void AddOldWindows (TreeMap<Long, HashMap<String, Window>> passedTreeMap)
	{
		while (!passedTreeMap.isEmpty())
		{
			long curPassedKey = passedTreeMap.firstKey();
			
			if (curPassedKey <= timeLimit)
			{
				HashMap<String, Window> curPassedHashMap = passedTreeMap.get(curPassedKey);
				
				for (Window w : curPassedHashMap.values()) { w.finishCalc(); }
				
				if (windowMap.containsKey(curPassedKey))
				{
					LinkedList<Window> curList = windowMap.get(curPassedKey);
					curList.addAll(passedTreeMap.firstEntry().getValue().values());
				}
				else
				{
					LinkedList<Window> curList = new LinkedList<Window>();
					curList.addAll(passedTreeMap.firstEntry().getValue().values());
					windowMap.put(curPassedKey, curList);
				}
				passedTreeMap.remove(curPassedKey);
			}
			else
			{
				break;
			}
		}
	}
}



