package simpledb.parse;

import java.util.*;

import simpledb.materialize.*;
import simpledb.materialize.SumFn;

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
   private List<List<String>> groupFields;

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
   }

   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<List<String>> sortFields) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortFields = sortFields;
   }

   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<List<String>> sortFields,
         List<List<String>> groupFields) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortFields = sortFields;
      this.groupFields = groupFields;
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
   public List<String> groupFields() {
      List<String> groupFieldsResult = new ArrayList<>();
      for (List<String> groupField : groupFields) {
         groupFieldsResult.add(groupField.get(1));
      }

      return groupFieldsResult;
   }

   /**
    * Returns the aggregate functions for each group field.
    * 
    * @return a list of aggregate functions
    */
   public List<AggregationFn> aggFns() {
      List<AggregationFn> aggFnsRes = new ArrayList<>();
      for (List<String> groupField : groupFields) {
         String aggFnName = groupField.get(0);
         String col = groupField.get(1);
         if (aggFnName.equals("sum")) {
            AggregationFn aggFn = new SumFn(col);
            aggFnsRes.add(aggFn);
         } else if (aggFnName.equals("count")) {
            AggregationFn aggFn = new CountFn(col);
            aggFnsRes.add(aggFn);
         } else if (aggFnName.equals("avg")) {
            AggregationFn aggFn = new AvgFn(col);
            aggFnsRes.add(aggFn);
         } else if (aggFnName.equals("min")) {
            AggregationFn aggFn = new MinFn(col);
            aggFnsRes.add(aggFn);
         } else if (aggFnName.equals("max")) {
            AggregationFn aggFn = new MaxFn(col);
            aggFnsRes.add(aggFn);
         }
      }

      return aggFnsRes;
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
