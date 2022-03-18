package network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class TwoJoin {
  public static void main(String[] args) {
      String url = "jdbc:simpledb://localhost";
      String qry = "select sname1, sname2, sid1, sid2 "
            + "from testone, testtwo "
            + "where sid1 = sid2 ";
      
      final long startTime = System.currentTimeMillis();
      Driver d = new NetworkDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(qry)) {
         while (rs.next()) {
            String sname1 = rs.getString("sname1");
            String sname2 = rs.getString("sname2");
            int sid1 = rs.getInt("sid1");
            int sid2 = rs.getInt("sid2");
            System.out.println(sname1 + "\t" + sname2 + "\t" + sid1 + "\t" + sid2);
         }
      }
      catch(Exception e) {
         e.printStackTrace();
      }
      final long endTime = System.currentTimeMillis();
      System.out.println("Total execution time: " + (endTime - startTime));
   }
}
