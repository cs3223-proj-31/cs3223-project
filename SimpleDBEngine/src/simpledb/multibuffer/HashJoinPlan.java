package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.Predicate;
import simpledb.record.Schema;

public class HashJoinPlan implements Plan {
	private Transaction tx;
	private Plan lhs, rhs;
	private Schema schema = new Schema();
	private Predicate pred;
	
	public HashJoinPlan(Transaction tx, Plan lhs, Plan rhs, Predicate pred) {
		this.tx = tx;
		this.lhs = lhs;
		this.rhs = rhs;
		this.pred = pred;
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
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

}
