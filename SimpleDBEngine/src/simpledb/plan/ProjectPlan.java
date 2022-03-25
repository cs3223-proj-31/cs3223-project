package simpledb.plan;

import java.util.List;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.materialize.DistinctScan;
import simpledb.query.*;

/**
 * The Plan class corresponding to the <i>project</i>
 * relational algebra operator.
 * 
 * @author Edward Sciore
 */
public class ProjectPlan implements Plan {
   private Plan p;
   private Schema schema = new Schema();
   private List<String> fieldlist;
   private boolean distinct;
   private Transaction tx;

   /**
    * Creates a new project node in the query tree,
    * having the specified subquery and field list.
    * 
    * @param p         the subquery
    * @param fieldlist the list of fields
    */
   public ProjectPlan(Plan p, List<String> fieldlist) {
      this.p = p;
      this.fieldlist = fieldlist;
      for (String fldname : fieldlist)
         schema.add(fldname, p.schema());
      this.distinct = false;
   }


   public ProjectPlan(Plan p, List<String> fieldlist, boolean distinct, Transaction tx) {
      this.p = p;
      for (String fldname : fieldlist)
         schema.add(fldname, p.schema());
      this.distinct = distinct;
      this.tx = tx;
   }

   /**
    * Creates a project scan for this query.
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      if(distinct) return new DistinctScan(new ProjectScan(s, schema.fields()), schema, tx);
      return new ProjectScan(s, schema.fields());
   }

   /**
    * Estimates the number of block accesses in the projection,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }

   /**
    * Estimates the number of output records in the projection,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Estimates the number of distinct field values
    * in the projection,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Returns the schema of the projection,
    * which is taken from the field list.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

   public String toString() {
      return "Project: " + fieldlist.toString() + " [" + p.toString() + "]";
   }
}
