package embedded;
import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class CustomQuery {
   public static void main(String[] args) {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select SName, DName, DId "
                 + "from DEPT, STUDENT "
                 + "where MajorId = DId "
                 + "and DId <> 20 ";

      Driver d = new EmbeddedDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         
         System.out.println("Name\tMajor\tDId");
         ResultSet rs = stmt.executeQuery(qry);
         while (rs.next()) {
            String sname = rs.getString("SName");
            String dname = rs.getString("DName");
            int did = rs.getInt("DId");
            System.out.println(sname + "\t" + dname + "\t" + did);
         }
         rs.close();
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}

