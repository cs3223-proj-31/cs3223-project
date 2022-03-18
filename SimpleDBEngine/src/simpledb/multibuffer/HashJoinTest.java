package simpledb.multibuffer;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class HashJoinTest {
	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();
		
		// Get plans here.
		Plan courseplan = new TablePlan(tx, "course", mdm);
		Plan sectionplan = new TablePlan(tx, "section", mdm);
		Plan hashjoinplan = new HashJoinPlan(
				            		tx,
				            		courseplan,
				            		sectionplan,
				            		"cid",
				            		"courseid"
				            	);
		
		Scan hashjoinscan = hashjoinplan.open();
		
		System.out.println("ACTUAL:");
		while (hashjoinscan.next()) {
			System.out.println(hashjoinscan.getString("title"));
		}
		hashjoinscan.close();
		
		tx.commit();
		
		System.out.println();
		
		String[] expectedtitles = {"calculus", "calculus", "db systems", "db systems", "elocution"};
		
		System.out.println("EXPECTED:");
		for (String title : expectedtitles) {
			System.out.println(title);
		}
	}
}
