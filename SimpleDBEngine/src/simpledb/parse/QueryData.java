package simpledb.parse;

import java.util.*;

import simpledb.materialize.*;

import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<List<String>> sortFields;
   private List<String> groupByfields;
   private List<AggregationFn> aggFns;
   private List<String> allFields;

   /**
    * Saves the field and table list and predicate.
    */
   // public QueryData(List<String> fields, Collection<String> tables, Predicate
   // pred) {
   // this.fields = fields;
   // this.tables = tables;
   // this.pred = pred;
   // }

   // public QueryData(List<String> fields, Collection<String> tables, Predicate
   // pred, List<List<String>> sortFields) {
   // this.fields = fields;
   // this.tables = tables;
   // this.pred = pred;
   // this.sortFields = sortFields;
   // }

   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<List<String>> sortFields,
         List<String> groupByfields) {
      setFields(fields);
      this.tables = tables;
      this.pred = pred;
      this.sortFields = sortFields;
      this.groupByfields = groupByfields;

      // System.out.println("QUERYDATA CREATED");
      // System.out.println("DATA:" + this.fields.toString());
   }

   /**
    * Returns the fields mentioned in the select clause.
    * 
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the fields mentioned in the select clause (includes aggregated
    * fields).
    * 
    * @return a list of field names
    */
   public List<String> allFields() {
      return allFields;
   }

   /**
    * Returns the sort fields mentioned in the select clause.
    * 
    * @return a list of sort field names
    */
   public List<List<String>> sortFields() {
      return sortFields;
   }

   /**
    * Returns the group fields mentioned in the select clause.
    * 
    * @return a list of group field names
    */
   public List<String> groupByFields() {
      return groupByfields;
   }

   /**
    * Returns the aggregate functions.
    * 
    * @return a list of agrgegate functions
    */
   public List<AggregationFn> aggFns() {
      return aggFns;
   }

   /**
    * Returns the aggregate functions for each group field.
    * 
    * @return a list of aggregate functions
    */
   public void setFields(List<String> fields) {
      // System.out.println("IN SET FIELDS: RUNNING");

      List<AggregationFn> aggFnsRes = new ArrayList<>();
      // List<String> normalFields = new ArrayList<>();
      // allFields = fields;

      for (String field : fields) {
         // System.out.println("IN SET FIELDS: " + field);

         if (field.startsWith("sum")) {
            // System.out.println("SUM DETECTED: " + field);

            String col = field.substring(5, field.length());
            // System.out.println("COL: " + col);
            // normalFields.add(col);

            AggregationFn aggFn = new SumFn(col);
            aggFnsRes.add(aggFn);
         } else if (field.startsWith("count")) {
            String col = field.substring(7, field.length());
            // normalFields.add(col);

            AggregationFn aggFn = new CountFn(col);
            aggFnsRes.add(aggFn);
         } else if (field.startsWith("avg")) {
            String col = field.substring(5, field.length());
            // normalFields.add(col);

            AggregationFn aggFn = new AvgFn(col);
            aggFnsRes.add(aggFn);
         } else if (field.startsWith("min")) {
            String col = field.substring(5, field.length());
            // normalFields.add(col);

            AggregationFn aggFn = new MinFn(col);
            aggFnsRes.add(aggFn);
         } else if (field.startsWith("max")) {
            String col = field.substring(5, field.length());
            // normalFields.add(col);

            AggregationFn aggFn = new MaxFn(col);
            aggFnsRes.add(aggFn);
         } else {
            // Is a normal field (no aggregation functions applied)
            // normalFields.add(field);
         }
      }

      aggFns = aggFnsRes;
      this.fields = fields;
      // this.fields = normalFields;
   }

   /**
    * Returns the tables mentioned in the from clause.
    * 
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }

   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * 
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
