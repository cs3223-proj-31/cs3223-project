package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>avg</i> aggregation function.
 * 
 * @author Edward Sciore
 */
public class AvgFn implements AggregationFn {
    private String fldname;
    private int sum;
    private int count;

    /**
     * Create a avg aggregation function for the specified field.
     * 
     * @param fldname the name of the aggregated field
     */
    public AvgFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new avg.
     * Since SimpleDB does not support null values,
     * every record will be counted,
     * regardless of the field.
     * 
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    public void processFirst(Scan s) {
        sum = s.getInt(fldname);
        count = 1;
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always adds to the sum,
     * regardless of the field.
     * 
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    public void processNext(Scan s) {
        sum += s.getInt(fldname);
        count++;
    }

    /**
     * Return the field's name, prepended by "avgof".
     * 
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "avgof" + fldname;
    }

    /**
     * Return the current avg.
     * 
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        // Constant is either string or int
        int avg = sum / count;
        return new Constant(avg);
    }
}
