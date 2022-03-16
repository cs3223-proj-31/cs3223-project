package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
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
	private String joinfield;
	private MetadataMgr md;
	private Schema schema = new Schema();
	private boolean doesBigPartExist;
	
	public HashJoinPlan(Transaction tx, Plan lhs, Plan rhs, String joinfield, MetadataMgr md) {
		this.tx = tx;
		this.lhs = lhs;
		this.rhs = rhs;
		this.joinfield = joinfield;
		this.md = md;
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		doesBigPartExist = false;
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
		
		// Recursively hashjoin if at least one partition cannot fit into
		// memory.
		if (doesBigPartExist) {
			TablePlan lhstp = new TablePlan(tx, lhstts[0].tableName(), md);
			TablePlan rhstp = new TablePlan(tx, rhstts[0].tableName(), md);
			HashJoinPlan recursiveplan = new HashJoinPlan(tx, lhstp, rhstp, joinfield, md);
			
			for (int i = 1; i < lhstts.length; i++) {
				// TODO try to use a plan that just scans through two tables
				// w/o partitioning.
				recursiveplan = new HashJoinPlan(
									tx,
									recursiveplan,
									new HashJoinPlan(
											tx,
											new TablePlan(tx, lhstts[i].tableName(), md),
											new TablePlan(tx, rhstts[i].tableName(), md),
											joinfield,
											md
									),
									joinfield,
									md
								);
			}
			
			return recursiveplan.open();
		} else {
			Scan[] ss1 = new Scan[k];
			Scan[] ss2 = new Scan[k];
			
			for (int i = 0; i < k; i++) {
				ss1[i] = lhstts[i].open();
				ss2[i] = rhstts[i].open();
			}
			
			return new HashJoinScan(ss1, ss2, joinfield, tx, lhs.schema());
		}
	}

	@Override
	public int blocksAccessed() {
		return 3 * (lhs.blocksAccessed() + rhs.blocksAccessed());
	}

	@Override
	public int recordsOutput() {
	      int maxvals = Math.max(lhs.distinctValues(joinfield),
	    		  		rhs.distinctValues(joinfield));
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
		int[] numrecspertable = new int[k];  // Number of records per table.
		
		// Open a scan for k temp tables.
		for (int i = 0; i < k; i++) {
			uss[i] = tts[i].open();
		}
		
		Scan scan = p.open();
		Schema sch = p.schema();
		
		// For each record of p...
		while (scan.next()) {
			joinfldval = scan.getVal(joinfield);
			// ...hash the record's join field to get h...
			h = hashWithinK(k, joinfldval);
			// ...and copy the record to the h-th temp table.
			uss[h].insert();
			for (String fldname : sch.fields()) {
				uss[h].setVal(fldname, scan.getVal(fldname));
			}
			
			// Big (size > k) partition check.
			numrecspertable[h]++;
			if (numrecspertable[h] > k) {
				doesBigPartExist = true;
			}
		}
		
		// Close the temp scans.
		for (UpdateScan us : uss) {
			us.close();
		}
		
		scan.close();
		
	}
}
