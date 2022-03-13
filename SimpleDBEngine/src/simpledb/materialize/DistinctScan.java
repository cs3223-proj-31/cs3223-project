package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;

import simpledb.record.Schema;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

/**
 * The scan class corresponding to the distinct filtering of an underlying
 * project scan
 * 
 * @author Andrew
 */
public class DistinctScan implements Scan {
   private Scan s;
   private RecordComparator comp;
   private Transaction tx;
   private Schema sch;

   public DistinctScan(Scan src, Schema schema, Transaction tx) {
      this.sch = schema;
      comp = new RecordComparator(schema.fields());
      List<TempTable> runs = splitIntoRuns(src);
      this.tx = tx;
      src.close();
      while (runs.size() > 1)
         runs = doAMergeIteration(runs);
      s = new SortScan(runs, comp);
   }

   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(tx, sch);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
            // start a new run
            currentscan.close();
            currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            currentscan = (UpdateScan) currenttemp.open();
         }
      currentscan.close();
      return temps;
   }

   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }

   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(tx, sch);
      UpdateScan dest = result.open();

      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
            hasmore1 = copy(src1, dest);
         else if (comp.compare(src1, src2) == 0) {
            hasmore1 = src1.next();
         }
         else
            hasmore2 = copy(src2, dest);

      if (hasmore1)
         while (hasmore1)
            hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
            hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }

   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }
   
   public void beforeFirst() {
      s.beforeFirst();
   }
   
   public boolean next() {
      return s.next();
   }
   
   public int getInt(String fldname) {
      if (hasField(fldname))
         return s.getInt(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }
   
   public String getString(String fldname) {
      if (hasField(fldname))
         return s.getString(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }
   
   public Constant getVal(String fldname) {
      if (hasField(fldname))
         return s.getVal(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public boolean hasField(String fldname) {
      return sch.fields().contains(fldname);
   }
   
   public void close() {
      s.close();
   }
}
