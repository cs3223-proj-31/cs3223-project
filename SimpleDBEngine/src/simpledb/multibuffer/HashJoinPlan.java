package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.query.Constant;
import simpledb.record.Schema;

/**
 * This class follows the definition of hash joins as mentioned in
 * Section 14.7 of Sciore (2020). Note that in this simulation we
 * only implement 2-step hashing, once here and once in hashjoinscan.
 * @author riyadh-h
 *
 */
public class HashJoinPlan implements Plan {
	private Transaction tx;
	private Plan lhs, rhs;  // p1 corresponds to T1; similar for p2 and T2.
	private String lhsjoinfld, rhsjoinfld;
	private Schema schema = new Schema();
	
	/**
    * Creates a hashjoin plan for the two specified queries.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    */
	public HashJoinPlan(Transaction tx, Plan lhs, Plan rhs, String lhsjoinfld, String rhsjoinfld) {
		this.tx = tx;
		this.lhs = lhs;
		this.rhs = rhs;
		this.lhsjoinfld = lhsjoinfld;
		this.rhsjoinfld = rhsjoinfld;
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
	}

	/** 
	 * The method identifies the appropriate number of buckets (K) to use based on
	 * the number of buffers available (B-1). It then hashes both lhs and rhs 
	 * into K buckets in the form of temptables, and returns a hashjoinscan that joins
	 * the partitions together.
	 */
	@Override
	public Scan open() {
		// Partition each of the two tables into as many partitions as
		// possible.
		int k = tx.availableBuffs() - 1;
		TempTable[] lhstts = new TempTable[k];
		TempTable[] rhstts = new TempTable[k];
		initTempTableArray(lhstts, lhs.schema());
		initTempTableArray(rhstts, rhs.schema());
		hashDistributeRecords(lhs, lhstts);
		hashDistributeRecords(rhs, rhstts);
		
		return new HashJoinScan(lhstts, rhstts, lhsjoinfld, rhsjoinfld, tx, lhs.schema());
	}

	/**
    * Return the number of block acceses required to
    * hashjoin the sorted tables.
    * We read all |LHS| + |RHS| pages, write out 
		* |LHS| + |RHS| pages of grace-hash join buckets,
		* then read |LHS| + |RHS| pages of grace-hash 
		* join buckets for the final join step.
		* Thus cost = 3 * (|LHS| + |RHS|)
    *
    * It <i>does</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
	@Override
	public int blocksAccessed() {
		return 3 * (lhs.blocksAccessed() + rhs.blocksAccessed());
	}

	/**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
	@Override
	public int recordsOutput() {
	      int maxvals = Math.max(lhs.distinctValues(lhsjoinfld),
	    		  		rhs.distinctValues(rhsjoinfld));
	      return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
	}

	/**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
	@Override
	public int distinctValues(String fldname) {
		if (lhs.schema().hasField(fldname)) {
			return lhs.distinctValues(fldname);
		} else {
			return rhs.distinctValues(fldname);
		}
	}

	/**
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
	@Override
	public Schema schema() {
		return schema;
	}
	
	@Override
	public String toString() {
		return "(" + lhs.toString() + " hash join " + rhs.toString() + ")";
	}
	
	/**
    * Initialises grace-hash partitions in the form of temptables
		* in an array of length k.
    */
	private void initTempTableArray(TempTable[] tts, Schema sch) {
		for (int i = 0; i < tts.length; i++) {
			tts[i] = new TempTable(tx, sch);
		}
	}
	
	/**
	 * Does the partitioning of underlying plan p into bucket temptables
	 * in tts
	 * 
	 * @param p the underlying plan to be hashed
	 * @param tts the array of k buckets
	 */
	private void hashDistributeRecords(Plan p, TempTable[] tts) {
		int h, k = tts.length;
		Constant joinfldval;
		UpdateScan[] uss = new UpdateScan[k];
		
		// Open a scan for k temp tables.
		for (int i = 0; i < k; i++) {
			uss[i] = tts[i].open();
		}
		
		Scan scan = p.open();
		Schema sch = p.schema();
		String joinfld = sch.hasField(lhsjoinfld)
						 	? lhsjoinfld
						 	: rhsjoinfld;
		
		// For each record of p...
		while (scan.next()) {
			joinfldval = scan.getVal(joinfld);
			// ...hash the record's join field to get h...
			h = hashWithinK(k, joinfldval);
			// ...and copy the record to the h-th temp table.
			uss[h].insert();
			for (String fldname : sch.fields()) {
				uss[h].setVal(fldname, scan.getVal(fldname));
			}
		}
		
		// Close the temp scans.
		for (UpdateScan us : uss) {
			us.close();
		}
		
		scan.close();
		
	}
	
	/**
	 * Hash function used to distribute records among k buckets.
	 * 
	 * @param k number of buckets
	 * @param val key of record to be hashed.
	 * @return
	 */
	private int hashWithinK(int k, Constant val) {
		return val.hashCode() % k;
	}
}
