package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import simpledb.materialize.*;
import simpledb.plan.Plan;


public class BlockNestedJoinPlan implements Plan {
   private Transaction tx;
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema schema = new Schema();

   /**
    * Creates a block nested join plan for the specified queries.
    * @param p1 the plan for the p1 query
    * @param p2 the plan for the p2 query
    * @param tx the calling transaction
    * @param fldname1 LHS field name used in join
    * @param fldname2 RHS field name used in join
    */
   public BlockNestedJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
	this.tx = tx;
    this.fldname1 = fldname1;
    this.p1 = new MaterializePlan(tx, p1);

    this.fldname2 = fldname2;
    this.p2 = p2;

    schema.addAll(p1.schema());
    schema.addAll(p2.schema());
   }

   /**
    * A scan for this query is created and returned, as follows.
    * First, the method materializes its p1 and p2 queries.
    * It then determines the optimal chunk size,
    * based on the size of the materialized p2 file and the
    * number of available buffers.
    * It creates a chunk plan for each chunk, saving them in a list.
    * Finally, it creates a multiscan for this list of plans,
    * and returns that scan.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan leftscan = p1.open();
      TempTable tt = copyRecordsFrom(p2);
      return new BlockNestedJoinScan(tx, leftscan, tt.tableName(), tt.getLayout(), fldname1, fldname2);
   }

   /**
    * Returns an estimate of the number of block accesses
    * required to execute the query. The formula is:
    * <pre> B(join(p1,p2)) = B(p2) + B(p1)*C(p2) </pre>
    * where C(p2) is the number of chunks of p2.
    * The method uses the current number of available buffers
    * to calculate C(p2), and so this value may differ
    * when the query scan is opened.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // this guesses at the # of chunks
      int avail = tx.availableBuffs();
      int size = new MaterializePlan(tx, p2).blocksAccessed();
      int numchunks = size / avail;
      return p2.blocksAccessed() +
            (p1.blocksAccessed() * numchunks);
   }

  /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
      p2.distinctValues(fldname2));
      return (p1.recordsOutput()* p2.recordsOutput()) / maxvals;
   }

 /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }

    /**
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }
   
   @Override
   public String toString() {
	   return "(" + p1.toString() + " block nested join " + p2.toString() + ")";
   }

   // effectively materialise.open(), but return temptable instead of scan
   // since dont need to delay with materialiseplan
   private TempTable copyRecordsFrom(Plan p) {
      Scan   src = p.open(); 
      Schema sch = p.schema();
      TempTable t = new TempTable(tx, sch);
      UpdateScan dest = (UpdateScan) t.open();
      while (src.next()) {
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      src.close();
      dest.close();
      return t;
   }
}
