package simpledb.opt;

import java.util.Map;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.metadata.*;
import simpledb.multibuffer.BlockNestedJoinPlan;
import simpledb.multibuffer.BlockNestedJoinScan;
import simpledb.multibuffer.HashJoinPlan;
import simpledb.plan.*;
import simpledb.query.*;
import simpledb.index.*;
import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.query.IndexJoinScan;
import simpledb.materialize.MergeJoinPlan;

// Find the grades of all students.

public class JoinTests {
  public static void main(String[] args) {
    SimpleDB db = new SimpleDB("studentdb");
      MetadataMgr mdm = db.mdMgr();
      Transaction tx = db.newTx();
      
    // Find the index on StudentId.
    Map<String,IndexInfo> indexes = mdm.getIndexInfo("testone", tx);
    IndexInfo sidIdx = indexes.get("sid1");

    // Get plans for the Student and Enroll tables
    final long startTime = System.currentTimeMillis();
    Plan idoneplan = new TablePlan(tx, "testone", mdm);
    Plan idtwoplan = new TablePlan(tx, "testtwo", mdm);
    Plan idthreeplan = new TablePlan(tx, "testthree", mdm);
    Plan idfourplan = new TablePlan(tx, "testfour", mdm);
    Plan joinplan1 = new IndexJoinPlan(idoneplan, idtwoplan, sidIdx, "sid1");
    Plan joinplan2 = new IndexJoinPlan(joinplan1, idthreeplan, sidIdx, "sid2");
    Plan joinplan3 = new IndexJoinPlan(joinplan2, idfourplan, sidIdx, "sid3");
    Scan s = joinplan.open();
    while (s.next());
    tx.commit();
    final long endTime = System.currentTimeMillis();
    System.out.println("Total execution time: " + (endTime - startTime));
  }

  private static void useIndexScan(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
    // Open an index join scan on the table.
    Plan idxplan = new IndexJoinPlan(p1, p2, ii, joinfield);
    Scan s = idxplan.open();

    while (s.next()) {
      System.out.println(s.getString("sname1") + s.getString("sname2") + s.getInt("sid1") + s.getInt("sid2"));
    }
    s.close();
  }
}