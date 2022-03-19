package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * 
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private RecordComparator comp;
   private List<String> sortfields;
   private List<List<String>> sortfieldsReal;

   /**
    * Create a sort plan for the specified query.
    * 
    * @param p          the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx         the calling transaction
    */
   public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      this.sortfields = sortfields;
      comp = new RecordComparator(sortfields);
   }

   // Extra issort param to escape type erasure error
   public SortPlan(Transaction tx, Plan p, List<List<String>> sortfields, boolean isSort) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      this.sortfieldsReal = sortfields;
      comp = new RecordComparator(sortfields, true);
   }

   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > 1)
         runs = doAMergeIteration(runs);
      return new SortScan(runs, comp);
   }

   /**
    * Return the number of page I/Os used to process the 2-way merge sort.
    * This includes the number of page I/Os used to materialise and sort the
    * underlying plans. Due to the wildly unpredictable nature of the greedy
    * method used to generate initial runs, we elected to arbitrarily use
    * planSize/2 as the number of initial starting runs, generously assuming
    * that each starting run contains 2 pages worth of records.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      int planSize = mp.blocksAccessed();
      return 2 * planSize * (int)(1+Math.log(planSize/2));
   }

   /**
    * Return the number of records in the sorted table,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Return the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Return the schema of the sorted table, which
    * is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
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

   public String toString() {
      // Either sortfields or sortfieldsReal will be empty, so won't have duplicates
      return "Sort by: " + this.sortfields.toString() + this.sortfieldsReal.toString();
   }
}
