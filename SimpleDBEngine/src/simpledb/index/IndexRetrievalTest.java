package simpledb.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;

import simpledb.metadata.*;
import simpledb.plan.*;
import simpledb.query.*;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class IndexRetrievalTest {
   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("studentdb");
      Transaction tx = db.newTx();
      MetadataMgr mdm = db.mdMgr();

      // Open a scan on the data table.
      Plan studentplan = new TablePlan(tx, "student", mdm);
      UpdateScan studentscan = (UpdateScan) studentplan.open();

      // Open the index on MajorId.
      Map<String, IndexInfo> indexes = mdm.getIndexInfo("student", tx);
      IndexInfo ii = indexes.get("majorid");
      Index idx = ii.open();

      ArrayList<String> resArray = new ArrayList<>();
      TreeSet<String> expected = new TreeSet<>(Arrays.asList("amy", "sue", "kim", "pat"));

      // Retrieve all index records having a dataval of 20.
      idx.beforeFirst(new Constant(20));
      while (idx.next()) {
         // Use the datarid to go to the corresponding STUDENT record.
         RID datarid = idx.getDataRid();
         studentscan.moveToRid(datarid);
         System.out.println(studentscan.getString("sname"));

         resArray.add(studentscan.getString("sname"));
      }
      
      TreeSet<String> res = new TreeSet<>(resArray);

      if (!res.equals(expected)) {
         throw new RuntimeException("TEST FAILED");
      }

      System.out.println("TEST PASSED");

      // Close the index and the data table.
      idx.close();
      studentscan.close();
      tx.commit();
   }
}
