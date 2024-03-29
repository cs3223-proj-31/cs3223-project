package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.HashJoinPlan;
import simpledb.multibuffer.BlockNestedJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use choose an optimal
    * join algorithm between nested loops, merge join and
    * index join. Optimality is determined by least blocks accessed.
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
      System.out.println("attempting index join");
      Plan p = makeIndexJoin(current, currsch);
      Plan blockJoin = makeBlockNestedJoin(current, currsch);
      if (p == null || p.blocksAccessed() > blockJoin.blocksAccessed()){
         System.out.println("nested preferred");
         p = makeProductJoin(current, currsch);
      }
      Plan mergeJoin = makeMergeJoin(current, currsch);
      if (p == null || mergeJoin != null && p.blocksAccessed() > mergeJoin.blocksAccessed()) {
         System.out.println("merge preferred");
         p = mergeJoin;
      }
      Plan hashJoin = makeHashJoin(current, currsch);
      if (p == null || hashJoin != null && p.blocksAccessed() > hashJoin.blocksAccessed()) {
    	  System.out.println("hashjoin preferred");
    	  p = hashJoin;
      }
      return p;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   private Plan makeBlockNestedJoin(Plan current, Schema currsch){
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            Plan p = new BlockNestedJoinPlan(tx, myplan, current, fldname, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   private Plan makeMergeJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            Plan p = new MergeJoinPlan(tx, myplan, current, fldname, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan makeHashJoin(Plan current, Schema currsch) {
	   for (String fldname : myschema.fields()) {
	         String outerfield = mypred.equatesWithField(fldname);
	         if (outerfield != null && currsch.hasField(outerfield)) {
	            Plan p = new HashJoinPlan(tx, myplan, current, fldname, outerfield);
	            p = addSelectPred(p);
	            return addJoinPred(p, currsch);
	         }
	      }
	      return null;
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
