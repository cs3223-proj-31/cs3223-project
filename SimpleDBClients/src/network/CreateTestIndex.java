package network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class CreateTestIndex {
   public static void main(String[] args) {
      Driver d = new NetworkDriver();
      String url = "jdbc:simpledb://localhost";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create index idxone on TESTONE(SId1) using btree";
         stmt.executeUpdate(s);
         System.out.println("TESTONE btree(sid1) created");

         s = "create index idxone on TESTTWO(SId2) using btree";
         stmt.executeUpdate(s);
         System.out.println("TESTTWO btree(sid2) created");
         
         s = "create index idxone on TESTTHREE(SId3) using btree";
         stmt.executeUpdate(s);
         System.out.println("TESTTHREE(SId3) btree created");
         
         s = "create index idxone on TESTFOUR(SId4) using btree";
         stmt.executeUpdate(s);
         System.out.println("TESTFOUR(SId4) btree created");
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
