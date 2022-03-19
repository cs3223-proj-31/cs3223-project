package simpledb.set;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class SetUnionPlan implements Plan {
	private Plan lhsplan;
	private Plan rhsplan;

	@Override
	public Scan open() {
		return new SetUnionScan(lhsplan.open(), rhsplan.open(), lhsplan.schema(), rhsplan.schema());
	}

	@Override
	public int blocksAccessed() {
		// TODO Auto-generated method stub
		return lhsplan.blocksAccessed() + rhsplan.blocksAccessed();
	}

	@Override
	public int recordsOutput() {
		// Average of the two plans' outputs.
		return (lhsplan.recordsOutput() + rhsplan.recordsOutput()) / 2;
	}

	@Override
	public int distinctValues(String fldname) {
		return lhsplan.distinctValues(fldname);
	}

	@Override
	public Schema schema() {
		// Assume both plans have the same schema.
		// The field names of the LHS plan will be used.
		return lhsplan.schema();
	}

}
