package network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class FourJoin {
  public static void main(String[] args) {
    String url = "jdbc:simpledb://localhost";
    String qry = "select sname1, sname2, sname3, sname4, sid1, sid2, sid3, sid4 "
          + "from testone, testtwo, testthree, testfour "
          + "where sid1 = sid2 and sid2 = sid3 and sid3 = sid4";
    
    final long startTime = System.currentTimeMillis();
    Driver d = new NetworkDriver();
    try (Connection conn = d.connect(url, null);
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(qry)) {
       while (rs.next()) {
          String sname1 = rs.getString("sname1");
          String sname2 = rs.getString("sname2");
          String sname3 = rs.getString("sname3");
          String sname4 = rs.getString("sname4");
          int sid1 = rs.getInt("sid1");
          int sid2 = rs.getInt("sid2");
          int sid3 = rs.getInt("sid3");
          int sid4 = rs.getInt("sid4");
          System.out.println(sname1 + "\t" + sname2 + "\t" + sname3 + "\t" + sname4 + "\t" + sid1 + "\t" + sid2 + "\t" + sid3 + "\t" + sid4);
       }
    }
    catch(Exception e) {
       e.printStackTrace();
    }
    final long endTime = System.currentTimeMillis();
    System.out.println("Total execution time: " + (endTime - startTime));
 }
}
