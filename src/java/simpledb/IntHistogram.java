package simpledb;

import java.lang.reflect.Array;
import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] histogram;
    private int buckets;
    private int min;
    private int max;
    private double interval;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.histogram = new int[buckets];
        for(int i = 0; i < buckets; i++)
            this.histogram[i] = 0;
        this.max = max;
        this.min = min;
        this.interval = (double) (max - min) / (double) buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v == max)
            this.histogram[buckets - 1]++;
        else {
            int index = (int)((v - min) / interval);
            this.histogram[index]++;
        }
        ntups++;

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int index = 0;
        if(v == max)
            index = buckets - 1;
        else
            index = (int)((v - min) / interval);
        double selectivity = 0;
        if(op == Predicate.Op.EQUALS){
            if(v < min || v > max) return 0;
            if(interval < 1) return (double)(histogram[index]) / (double)ntups;
            selectivity = (double)(histogram[index]) / interval / (double)ntups;
        }
        else if(op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ){
            if(v <= min) return 1;
            if(v >= max) return 0;
            for(int i = index + 1; i < buckets; i++)
                selectivity = selectivity + (double) histogram[i] /(double) ntups;
            double right = (double)(index + 1) * interval;
            selectivity = selectivity + (right - (double)v) / interval * (double)histogram[index] / (double)ntups;
            if(op == Predicate.Op.GREATER_THAN_OR_EQ)
                selectivity += (double)(histogram[index]) / interval / (double)ntups;
        }
        else if(op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {
            if(v <= min) return 0;
            if(v >= max) return 1;
            for(int i = 0; i < index; i++)
                selectivity = selectivity + (double) histogram[i] /(double) ntups;
            double left = (double)index * interval;
            selectivity = selectivity + ((double)v - left) / interval * (double)histogram[index] / (double)ntups;
            if(op == Predicate.Op.LESS_THAN_OR_EQ)
                selectivity += (double)(histogram[index]) / interval / (double)ntups;
        }
        else {
            if(v < min || v > max) return 1;
            double temp = (double)(histogram[index]) / interval / (double)ntups;
            return 1 - temp;
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return Arrays.toString(histogram);
    }
}
