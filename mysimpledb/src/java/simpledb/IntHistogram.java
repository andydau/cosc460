package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
	private int[] buckets;
	private int min;
	private int max;
	private int bucketSize;
	private int total;
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = (buckets<=max-min+1)? new int[buckets]:new int[max-min+1];
        this.min = min;
        this.max = max;
        this.bucketSize = (max-min+1)/this.buckets.length;
        this.total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if ((v>max)||(v<min)){
        	throw new RuntimeException();
        }
        int bucketNo = (v-min)/bucketSize;
        if (bucketNo>=this.buckets.length){
        	bucketNo = this.buckets.length-1;
        }
        this.total++;
        this.buckets[bucketNo]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucketNo = (v-min)/bucketSize;
    	if (bucketNo>=this.buckets.length){
        	bucketNo = this.buckets.length-1;
        }
    	if (bucketNo<0){
    		bucketNo = 0;
    	}
    	int bucketStart = min+bucketNo*this.bucketSize;
    	int bucketEnd = (bucketNo != this.buckets.length-1) ? bucketStart+this.bucketSize-1:max;
    	double equals = 0;
    	if ((bucketStart<=v)&&(v<=bucketEnd)){
    		equals = this.buckets[bucketNo]/(double)(bucketEnd-bucketStart+1);
    	}
    	double smaller = 0;
    	smaller += (v<=bucketEnd) ? equals*(v-bucketStart):this.bucketSize;
    	for (int i = 0; i < bucketNo; i++){
    		smaller += this.buckets[i];
    	}
    	double greater = 0;
    	greater += (v>=bucketStart) ? equals*(bucketEnd-v):this.bucketSize;
    	for (int i = bucketNo+1; i < this.buckets.length; i++){
    		greater += this.buckets[i];
    	}
    	switch (op.toString()){
    		case "=": return equals/this.total;
    		case "LIKE": return equals/this.total;
    		case ">": return greater/this.total;
    		case ">=": return (equals+greater)/this.total;
    		case "<": return smaller/this.total;
    		case "<=": return (smaller+equals)/this.total;
    		case "<>": return (smaller+greater)/this.total;
    	}
    	return 0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
