package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * 
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<String> fields;
   private List<List<String>> sortFields;

   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * 
    * @param fields a list of field names
    */

   public RecordComparator(List<String> fields) {
      this.fields = fields;
   }

   // Adding extra param to avoid type erasure error (with just using sortFields)
   public RecordComparator(List<List<String>> sortFields, boolean isSortField) {
      this.sortFields = sortFields;
   }

   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * 
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the
    *         field list
    */
   public int compare(Scan s1, Scan s2) {
      if (fields != null) {
         for (String fldname : fields) {
            Constant val1 = s1.getVal(fldname);
            Constant val2 = s2.getVal(fldname);
            int result = val1.compareTo(val2);
            if (result != 0)
               return result;
         }
         return 0;
      } else {
         for (List<String> sortField : sortFields) {
            String fldname = sortField.get(0);
            String order = sortField.get(1);

            Constant val1 = s1.getVal(fldname);
            Constant val2 = s2.getVal(fldname);

            int result = val1.compareTo(val2);

            if (order.equals("desc")) {
               result *= -1;
            }
            if (result != 0)
               return result;
         }
         return 0;
      }
   }
}