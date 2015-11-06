package mainPackage;


public class Tuple
{
	long time;						//Timestamp according to input data stream (relevant for calculation)
	long realtime;					//Timestamp according to clock (only relevant to measure latency)
	double dData;					//Double data for calculation purpose
	String sData;					//String data for groupBy operations
	boolean finalTuple = false;		//Marks the final tuple to be able to flush and close file writers.
	long lastTimeProcessed;			//Only needed temporary to measure the latency caused by each PU.
	
	
	public Tuple (long time, long realtime, double dData, String sData)
	{
		this.time = time;
		this.realtime = realtime;
		this.dData = dData;
		this.sData = sData;
	}
	
	
	public Tuple (long time, long realtime, double dData, String sData, long lastTimeProcessed)
	{
		this.time = time;
		this.realtime = realtime;
		this.dData = dData;
		this.sData = sData;
		this.lastTimeProcessed = lastTimeProcessed;
	}
	
	
	/*public Tuple (long time, double dData, String sData)
	{
		this.time = time;
		this.dData = dData;
		this.sData = sData;
	}*/
	
	
	/*public Tuple (Tuple anotherTuple)
	{
		this.time = anotherTuple.time;
		this.realtime = anotherTuple.realtime;
		this.dData = anotherTuple.dData;
		this.sData = anotherTuple.sData;
		this.finalTuple = anotherTuple.finalTuple;
	}*/
}
