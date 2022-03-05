package simpledb.materialize;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.plan.*;
import simpledb.query.*;

// Find the grades of all students.

public class MergeJoinTest {
	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
      MetadataMgr mdm = db.mdMgr();
      Transaction tx = db.newTx();

		// Get plans for the Student and Enroll tables
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);

		// Test merge join plans.
		Plan mergeJoinPlan = new MergeJoinPlan(tx, studentplan, enrollplan, "sid", "studentid");
		Scan s = mergeJoinPlan.open();
		while (s.next()) {
			System.out.println(s.getString("grade"));
		}

		tx.commit();
	}
}
