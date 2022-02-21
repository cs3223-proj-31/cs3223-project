package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

public class PlannerTest1 {
   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("plannertest1");
      Transaction tx = db.newTx();
      Planner planner = db.planner();
      String cmd = "create table T1(A int, B varchar(9), C varchar(9))";
      planner.executeUpdate(cmd, tx);

      int n = 10;
      System.out.println("Inserting " + n + " random records.");
      for (int i = 0; i < n; i++) {
         int a = (int) Math.round(Math.random() * 5);
         String b = "brec" + a;
         int c = (int) Math.round(Math.random() * 5);
         String d = "crec" + c;
         cmd = "insert into T1(A,B,C) values(" + a + ", '" + b + "', '" + d + "')";

         System.out.println(cmd);
         planner.executeUpdate(cmd, tx);
      }

      // Test cases
      // String qry = "select B from T1 where A<10";
      // B is asc
      // String qry = "select B, C from T1 where A<10 order by B";
      // B is asc
      // String qry = "select B, C from T1 where A<10 order by B asc";
      // B is desc
      // String qry = "select B, C from T1 where A<10 order by B desc";
      // B and C are asc
      // String qry = "select B, C from T1 where A<10 order by B, C";
      // B is asc, C is desc
      // String qry = "select B, C from T1 where A<10 order by B, C desc";
      // String qry = "select B, C from T1 where A<10 order by B asc, C desc";
      // B and C are desc
      // String qry = "select B, C from T1 where A<10 order by B desc, C desc";
      // B is desc, C is asc
      // String qry = "select B, C from T1 where A<10 order by B desc, C";
      // String qry = "select B, C from T1 where A<10 order by B desc, C asc";

      // Default
      String qry = "select B, C from T1 where A<10";
      Plan p = planner.createQueryPlan(qry, tx);
      Scan s = p.open();
      while (s.next())
         System.out.println("B:" + s.getString("b") + " C:" + s.getString("c"));
      s.close();
      tx.commit();
   }
}
