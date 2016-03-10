package mainPackage;


public class AggregatOperation
{
	static String[] operationName = {"Average", "Count", "Max", "Median", "Min", "Mode (most common number)", "Sum", "4x1KxCALC"};
	static final int OP_AVERAGE = 0, OP_COUNT = 1, OP_MAX = 2, OP_MEDIAN = 3, OP_MIN = 4, OP_MODE = 5, OP_SUM = 6, OP_4x1KxCALC = 7;
	int operationId;
	long windowSize;
	long windowStep;
	boolean groupBy;
	
	public AggregatOperation (int operationId, long windowSize, long windowStep, boolean groupBy)
	{
		this.operationId = operationId;
		this.windowSize = windowSize;
		this.windowStep = windowStep;
		this.groupBy = groupBy;
	}
}
