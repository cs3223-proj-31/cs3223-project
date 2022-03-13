package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.record.Schema;
import simpledb.record.TableScan;

/**
 * This class follows the definition of hash joins as mentioned in
 * Section 14.7 of Sciore (2020).
 * @author riyadh-h
 *
 */
public class HashJoinPlan implements Plan {
	private Transaction tx;
	private Plan p1, p2;  // p1 corresponds to T1; similar for p2 and T2.
	private Schema schema = new Schema();
	private Predicate pred;
	private String joinfield;
	
	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, Predicate pred) {
		this.tx = tx;
		this.p1 = p1;
		this.p2 = p2;
		this.pred = pred;
		schema.addAll(p1.schema());
		schema.addAll(p2.schema());
	}

	@Override
	public Scan open() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int distinctValues(String fldname) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		return null;
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
		}
		
		// Close the temp scans.
		for (UpdateScan us : uss) {
			us.close();
		}
		
		scan.close();
	}
}
