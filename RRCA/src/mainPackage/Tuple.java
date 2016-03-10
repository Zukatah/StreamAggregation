package mainPackage;


public class Tuple
{
	static int coreCount;
	
	long time;						//Timestamp according to input data stream (relevant for calculation)
	long realtime;					//Timestamp according to clock (only relevant to measure latency)
	double dData;					//Double data for calculation purpose
	String sData;					//String data for groupBy operations
	boolean finalTuple = false;		//Marks the final tuple to be able to flush and close file writers.
	long lastTimeProcessed;			//Only needed temporary to measure the latency caused by each PU.
	int firstThreadEntered;			//In the HO version we need to save, where each tupled entered the PU circle.
	int lastThreadEntered;			//Better save in tuple or calculate each time? I guess here in tuple, although bigger size
	
	
	
	public Tuple (long time, long realtime, double dData, String sData, long lastTimeProcessed, int firstThreadEntered)
	{
		this.time = time;
		this.realtime = realtime;
		this.dData = dData;
		this.sData = sData;
		this.lastTimeProcessed = lastTimeProcessed;
		this.firstThreadEntered = firstThreadEntered;
		this.lastThreadEntered = (firstThreadEntered - 1 + coreCount) % coreCount;
	}
}
