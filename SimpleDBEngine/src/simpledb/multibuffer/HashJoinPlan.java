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
 * Section 14.7 of Sciore (2020).
 * @author riyadh-h
 *
 */
public class HashJoinPlan implements Plan {
	private Transaction tx;
	private Plan lhs, rhs;  // p1 corresponds to T1; similar for p2 and T2.
	private String lhsjoinfld, rhsjoinfld;
	private Schema schema = new Schema();
	
	public HashJoinPlan(Transaction tx, Plan lhs, Plan rhs, String lhsjoinfld, String rhsjoinfld) {
		this.tx = tx;
		this.lhs = lhs;
		this.rhs = rhs;
		this.lhsjoinfld = lhsjoinfld;
		this.rhsjoinfld = rhsjoinfld;
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
	}

	@Override
	public Scan open() {
		// Partition each of the two tables into as many partitions as
		// possible.
		int k = tx.availableBuffs() - 1;
		TempTable[] lhstts = new TempTable[k];
		TempTable[] rhstts = new TempTable[k];
		hashDistributeRecords(lhs, lhstts);
		hashDistributeRecords(rhs, rhstts);
		
		Scan[] ss1 = new Scan[k];
		Scan[] ss2 = new Scan[k];
		
		for (int i = 0; i < k; i++) {
			ss1[i] = lhstts[i].open();
			ss2[i] = rhstts[i].open();
		}
		
		return new HashJoinScan(ss1, ss2, lhsjoinfld, rhsjoinfld, tx, lhs.schema());
	}

	@Override
	public int blocksAccessed() {
		return 3 * (lhs.blocksAccessed() + rhs.blocksAccessed());
	}

	@Override
	public int recordsOutput() {
	      int maxvals = Math.max(lhs.distinctValues(lhsjoinfld),
	    		  		rhs.distinctValues(rhsjoinfld));
	      return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
	}

	@Override
	public int distinctValues(String fldname) {
		if (lhs.schema().hasField(fldname)) {
			return lhs.distinctValues(fldname);
		} else {
			return rhs.distinctValues(fldname);
		}
	}

	@Override
	public Schema schema() {
		return schema;
	}
	
	private int hashWithinK(int k, Constant val) {
		return val.hashCode() % k;
	}
	
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
}
