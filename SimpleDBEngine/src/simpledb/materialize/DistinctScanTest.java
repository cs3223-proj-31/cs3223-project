package simpledb.materialize;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class DistinctScanTest {
  public static void main(String[] args) {
    SimpleDB db = new SimpleDB("studentdb");
    MetadataMgr mdm = db.mdMgr();
    Transaction tx = db.newTx();

      
    // Get plans for the Student table
    Plan studentplan = new TablePlan(tx, "student", mdm);
    Schema sch = new Schema();
    sch.add("majorid", studentplan.schema());

    // Open scans on the table.
    Scan s = studentplan.open();

		// Loop through s1 records. For each value of the join field, 
		// use the index to find the matching s2 records.
		

    // query for distinct col
	DistinctScan ds = new DistinctScan(s, sch, tx);
    while (ds.next()) {
			System.out.println(s.getVal("MajorId"));
		}
    
    // query for distinct pair of columns
    sch.add("GradYear", studentplan.schema());
    DistinctScan ds2 = new DistinctScan(s, sch, tx);
    while (ds2.next()) {
			System.out.println(s.getVal("MajorId").asString() + s.getVal("GradYear").asString());
		}
    tx.rollback();
  }
}
