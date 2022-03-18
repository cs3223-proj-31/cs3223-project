package simpledb.integration;

import simpledb.plan.Planner;
import simpledb.plan.Plan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

public class IntegrationTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("plannertest1");
        Transaction tx = db.newTx();
        Planner planner = db.planner();
        String cmd = "create table T1(A int, B varchar(9), C int)";
        planner.executeUpdate(cmd, tx);

        int n = 10;
        System.out.println("Inserting " + n + " records.");
        for (int i = 0; i < n; i++) {
            int a = i;
            int c = i % 2;
            String b = "brec" + c;
            cmd = "insert into T1(A,B,C) values(" + a + ", '" + b + "', " + c + ")";

            System.out.println(cmd);
            planner.executeUpdate(cmd, tx);
        }

        // Default
        String qry = "select B, sumofc from T1 where B >= 'brec1' group by B order by B";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s = p.open();
        while (s.next()) {
            System.out.println("b:" + s.getString("b"));
            System.out.println("sumofc:" + s.getInt("sumofc"));
        }

        s.close();

        tx.commit();
    }
}
