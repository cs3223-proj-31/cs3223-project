package network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class CreateTestDB {
   public static void main(String[] args) {
      Driver d = new NetworkDriver();
      String url = "jdbc:simpledb://localhost";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create table TESTONE(SName1 varchar(20), SId1 int)";
         stmt.executeUpdate(s);
         System.out.println("Table TESTONE created.");

         s = "insert into TESTONE(SName1, SId1 ) values ";
         String[] test1 = {"('Chase', 25)",
         "('Berk', 14)",
         "('Lionel', 46)",
         "('Helen', 4)",
         "('Haviva', 39)",
         "('Len', 3)",
         "('Madeson', 26)",
         "('Edward', 14)",
         "('Zoe', 35)",
         "('Lucius', 41)",
         "('Bernard', 20)",
         "('Lani', 8)",
         "('Shaine', 21)",
         "('Roary', 39)",
         "('Rhiannon', 40)",
         "('Danielle', 36)",
         "('Phoebe', 9)",
         "('Daquan', 39)",
         "('Maile', 20)",
         "('Boris', 37)",
         "('Owen', 35)",
         "('Quail', 24)",
         "('Dale', 45)",
         "('Alexandra', 21)",
         "('Cara', 43)",
         "('Lamar', 19)",
         "('Zenia', 49)",
         "('Paula', 23)",
         "('Kiona', 48)",
         "('Xantha', 22)",
         "('Kelsey', 31)",
         "('Rajah', 20)",
         "('Miriam', 12)",
         "('Zephania', 48)",
         "('Shad', 1)",
         "('Hedley', 28)",
         "('Walker', 35)",
         "('Elmo', 48)",
         "('Whoopi', 12)",
         "('Melissa', 42)",
         "('Dana', 7)",
         "('Byron', 42)",
         "('Willow', 5)",
         "('Christopher', 2)",
         "('Plato', 7)",
         "('Callie', 14)",
         "('Aquila', 24)",
         "('Harding', 20)",
         "('Patrick', 24)",
         "('Sonya', 4)",
         "('Britanni', 10)",
         "('Eagan', 2)",
         "('Avram', 44)",
         "('Chiquita', 19)",
         "('Maryam', 23)",
         "('Dai', 16)",
         "('Hilda', 1)",
         "('Caleb', 0)",
         "('Donovan', 27)",
         "('Mallory', 3)",
         "('Alec', 3)",
         "('Wade', 32)",
         "('Tanya', 6)",
         "('Ira', 45)",
         "('Upton', 10)",
         "('Debra', 13)",
         "('Mercedes', 35)",
         "('Denise', 34)",
         "('Lee', 23)",
         "('Darrel', 2)",
         "('Olga', 4)",
         "('Tiger', 0)",
         "('Lucian', 31)",
         "('Fitzgerald', 3)",
         "('Yardley', 37)",
         "('Lyle', 39)",
         "('Zachery', 11)",
         "('Maryam', 9)",
         "('Barry', 3)",
         "('Macon', 24)",
         "('Abdul', 44)",
         "('Sebastian', 32)",
         "('Wang', 32)",
         "('Emily', 36)",
         "('Delilah', 6)",
         "('Timothy', 25)",
         "('Marsden', 49)",
         "('Keefe', 10)",
         "('Lane', 28)",
         "('Zorita', 22)",
         "('Veda', 32)",
         "('Abraham', 38)",
         "('Ulysses', 32)",
         "('Deborah', 13)",
         "('Joseph', 40)",
         "('Thane', 48)",
         "('Jocelyn', 39)",
         "('Emi', 37)",
         "('Fritz', 2)",
         "('Isaac', 18)",
         };
         for (int i=0; i<test1.length; i++)
            stmt.executeUpdate(s + test1[i]);
         System.out.println("Test 1 records inserted.");
         
         s = "create table TESTTWO(SName2 varchar(20), SId2 int)";
         stmt.executeUpdate(s);
         System.out.println("Table TESTTWO created.");

         s = "insert into TESTTWO(SName2, SId2) values ";
         String[] test2 = {"('Harriet', 35)",
         "('Fuller', 1)",
         "('Alika', 35)",
         "('Duncan', 8)",
         "('Grady', 43)",
         "('Axel', 47)",
         "('Victor', 31)",
         "('Kimberly', 49)",
         "('Germaine', 43)",
         "('Justine', 14)",
         "('Abel', 34)",
         "('Ryder', 9)",
         "('Fuller', 34)",
         "('Macaulay', 32)",
         "('Shellie', 45)",
         "('Nathaniel', 1)",
         "('Zelenia', 7)",
         "('Wesley', 3)",
         "('Noelle', 36)",
         "('Kirby', 19)",
         "('John', 35)",
         "('Diana', 27)",
         "('Duncan', 5)",
         "('Josiah', 44)",
         "('Uta', 45)",
         "('Nissim', 2)",
         "('Ocean', 17)",
         "('Leslie', 30)",
         "('Aphrodite', 31)",
         "('Diana', 4)",
         "('Nolan', 37)",
         "('Fletcher', 14)",
         "('Margaret', 25)",
         "('Gabriel', 49)",
         "('Gage', 0)",
         "('Ciara', 49)",
         "('Len', 24)",
         "('Rowan', 35)",
         "('Fritz', 37)",
         "('Faith', 34)",
         "('Brady', 36)",
         "('Neve', 31)",
         "('Kimberly', 50)",
         "('Evan', 29)",
         "('Nehru', 44)",
         "('Rylee', 47)",
         "('Isabelle', 31)",
         "('Elton', 13)",
         "('Knox', 47)",
         "('Hyatt', 17)",
         "('Benedict', 16)",
         "('Orlando', 20)",
         "('Magee', 24)",
         "('Ruth', 21)",
         "('Wendy', 19)",
         "('Brianna', 29)",
         "('Cairo', 21)",
         "('Prescott', 13)",
         "('Madaline', 41)",
         "('Savannah', 13)",
         "('Bianca', 29)",
         "('Magee', 8)",
         "('Baxter', 12)",
         "('Sacha', 8)",
         "('Stephen', 27)",
         "('Amir', 30)",
         "('Hector', 10)",
         "('Chadwick', 25)",
         "('Ishmael', 2)",
         "('Lois', 43)",
         "('Kyle', 9)",
         "('Emily', 46)",
         "('Hayes', 40)",
         "('Hyatt', 13)",
         "('Amery', 48)",
         "('Edward', 11)",
         "('Keegan', 15)",
         "('Sara', 41)",
         "('Maisie', 3)",
         "('Jason', 26)",
         "('Lance', 11)",
         "('Gareth', 39)",
         "('Bevis', 41)",
         "('Vivien', 39)",
         "('Barclay', 7)",
         "('Olympia', 43)",
         "('Hope', 43)",
         "('Steel', 2)",
         "('Maggie', 15)",
         "('Timothy', 38)",
         "('Joseph', 22)",
         "('Baker', 3)",
         "('Bradley', 16)",
         "('Acton', 5)",
         "('Alana', 36)",
         "('Serina', 30)",
         "('Connor', 6)",
         "('Brittany', 12)",
         "('Charity', 22)",
         "('Tate', 49)",         
         };
         for (int i=0; i<test2.length; i++)
            stmt.executeUpdate(s + test2[i]);
         System.out.println("Test 2 records inserted.");

         s = "create table TESTTHREE(SName3 varchar(20), SId3 int)";
         stmt.executeUpdate(s);
         System.out.println("Table TESTTHREE created.");

         s = "insert into TESTTHREE(SName3, SId3) values ";
         String[] test3 = {"('Tanek', 30)",
         "('Alika', 35)",
         "('Xantha', 24)",
         "('Mari', 37)",
         "('Rana', 43)",
         "('Duncan', 40)",
         "('Shana', 27)",
         "('Graham', 18)",
         "('Erasmus', 46)",
         "('Abbot', 27)",
         "('Tatyana', 47)",
         "('Orson', 18)",
         "('Aline', 9)",
         "('Bryar', 36)",
         "('Moana', 38)",
         "('Ray', 42)",
         "('Fredericka', 24)",
         "('Adrian', 21)",
         "('Susan', 2)",
         "('Carson', 20)",
         "('Iona', 31)",
         "('Josephine', 36)",
         "('Lewis', 36)",
         "('Acton', 49)",
         "('Kylynn', 27)",
         "('Florence', 20)",
         "('Danielle', 47)",
         "('Matthew', 24)",
         "('Olivia', 26)",
         "('Oprah', 4)",
         "('Marvin', 26)",
         "('Danielle', 44)",
         "('Orli', 18)",
         "('Noelle', 42)",
         "('Maxine', 39)",
         "('Rae', 27)",
         "('Maile', 44)",
         "('Colorado', 21)",
         "('Amber', 48)",
         "('Ulric', 44)",
         "('Allegra', 21)",
         "('Germane', 31)",
         "('Susan', 24)",
         "('Walker', 36)",
         "('Timon', 8)",
         "('Sebastian', 19)",
         "('Holmes', 41)",
         "('Geraldine', 42)",
         "('Kamal', 38)",
         "('Ainsley', 25)",
         "('Maxine', 38)",
         "('Byron', 4)",
         "('Noelle', 22)",
         "('Lars', 40)",
         "('Hamilton', 12)",
         "('Lacy', 14)",
         "('TaShya', 10)",
         "('Tad', 37)",
         "('Ivor', 7)",
         "('Nelle', 20)",
         "('Castor', 21)",
         "('Lewis', 27)",
         "('Ashely', 38)",
         "('Eugenia', 4)",
         "('Alexa', 45)",
         "('Dai', 18)",
         "('Carly', 37)",
         "('Rose', 7)",
         "('Lacy', 14)",
         "('Frances', 2)",
         "('Aphrodite', 17)",
         "('Martina', 22)",
         "('Phyllis', 32)",
         "('Charlotte', 21)",
         "('Chancellor', 25)",
         "('Maite', 22)",
         "('Oprah', 36)",
         "('Giacomo', 43)",
         "('Nehru', 37)",
         "('Rhona', 7)",
         "('Allen', 43)",
         "('Isaiah', 21)",
         "('Aimee', 14)",
         "('Rhonda', 4)",
         "('Lester', 47)",
         "('Gail', 32)",
         "('Harriet', 3)",
         "('Kuame', 19)",
         "('Cheyenne', 12)",
         "('Dawn', 8)",
         "('Laith', 4)",
         "('John', 4)",
         "('Jelani', 47)",
         "('Orla', 37)",
         "('Ivana', 46)",
         "('Roanna', 38)",
         "('Abra', 45)",
         "('Yolanda', 18)",
         "('Hakeem', 25)",
         "('Joan', 32)",         
         };
         for (int i=0; i<test3.length; i++)
            stmt.executeUpdate(s + test3[i]);
         System.out.println("Test 3 records inserted.");

         s = "create table TESTFOUR(SName4 varchar(20), SId4 int)";
         stmt.executeUpdate(s);
         System.out.println("Table TESTFOUR created.");

         s = "insert into TESTFOUR(SName4, SId4) values ";
         String[] test4 = {"('Ezra', 49)",
         "('Virginia', 7)",
         "('Scarlet', 46)",
         "('Barbara', 41)",
         "('Alden', 12)",
         "('Dean', 34)",
         "('Wynne', 14)",
         "('Tamara', 39)",
         "('Theodore', 32)",
         "('Rogan', 49)",
         "('Jana', 24)",
         "('Quamar', 3)",
         "('Stephen', 33)",
         "('Hashim', 12)",
         "('Quon', 40)",
         "('Hashim', 20)",
         "('Alma', 28)",
         "('Aristotle', 26)",
         "('Maya', 42)",
         "('Rama', 42)",
         "('Charles', 4)",
         "('Riley', 40)",
         "('Howard', 27)",
         "('Zia', 10)",
         "('Yuri', 28)",
         "('Mia', 37)",
         "('Magee', 30)",
         "('April', 17)",
         "('Cassidy', 34)",
         "('Dacey', 44)",
         "('Charity', 12)",
         "('Jillian', 41)",
         "('Amal', 9)",
         "('Keegan', 16)",
         "('Steven', 28)",
         "('Penelope', 46)",
         "('Karly', 4)",
         "('Graiden', 46)",
         "('Thaddeus', 12)",
         "('Blake', 6)",
         "('Karen', 47)",
         "('Finn', 37)",
         "('Merritt', 24)",
         "('Richard', 24)",
         "('Risa', 8)",
         "('Joy', 17)",
         "('Fiona', 43)",
         "('Molly', 13)",
         "('Rhona', 26)",
         "('Brett', 8)",
         "('Herrod', 47)",
         "('Bruno', 29)",
         "('David', 46)",
         "('Phelan', 35)",
         "('Emma', 15)",
         "('Shelley', 35)",
         "('Micah', 45)",
         "('Phillip', 36)",
         "('Sydney', 10)",
         "('Isadora', 4)",
         "('Kyra', 35)",
         "('Brent', 5)",
         "('Charlotte', 15)",
         "('Quinn', 19)",
         "('Cathleen', 28)",
         "('Adria', 10)",
         "('Kuame', 25)",
         "('Garrett', 12)",
         "('Jarrod', 22)",
         "('Haviva', 35)",
         "('Samuel', 4)",
         "('Zorita', 13)",
         "('Keelie', 46)",
         "('Evelyn', 45)",
         "('Karina', 12)",
         "('Todd', 16)",
         "('Amanda', 19)",
         "('Callum', 7)",
         "('Francesca', 21)",
         "('Colorado', 33)",
         "('Hayden', 7)",
         "('Elijah', 45)",
         "('Sopoline', 30)",
         "('Jasmine', 12)",
         "('Cynthia', 29)",
         "('Stone', 5)",
         "('Andrew', 6)",
         "('Erica', 45)",
         "('Carl', 35)",
         "('Gretchen', 34)",
         "('Levi', 48)",
         "('Fulton', 15)",
         "('Colby', 40)",
         "('Aurelia', 30)",
         "('Eugenia', 24)",
         "('Ignatius', 24)",
         "('Donovan', 23)",
         "('Rose', 39)",
         "('Tashya', 28)",
         "('Cynthia', 49)",         
         };

         for (int i=0; i<test4.length; i++)
            stmt.executeUpdate(s + test4[i]);
         System.out.println("Test 4 records inserted.");

         s = "create table TESTFIVE(SName5 varchar(20), SId5 int)";
         stmt.executeUpdate(s);
         System.out.println("Table TESTFIVE created.");

         s = "insert into TESTFIVE(SName5, SId5) values ";
         String[] test5 = {"('Quintessa', 46)",
         "('Candace', 29)",
         "('Ursula', 38)",
         "('Gregory', 3)",
         "('Xandra', 10)",
         "('Jamalia', 2)",
         "('Drake', 27)",
         "('Imani', 35)",
         "('Oleg', 35)",
         "('Sawyer', 39)",
         "('Devin', 29)",
         "('Kyra', 8)",
         "('Larissa', 3)",
         "('Michelle', 45)",
         "('Colorado', 27)",
         "('Adrienne', 41)",
         "('Alan', 14)",
         "('Thor', 29)",
         "('Rina', 50)",
         "('Chadwick', 4)",
         "('Kirk', 43)",
         "('Dawn', 23)",
         "('Zelda', 12)",
         "('Rinah', 50)",
         "('Coby', 18)",
         "('Justina', 42)",
         "('Violet', 38)",
         "('Troy', 16)",
         "('Reuben', 10)",
         "('Irene', 11)",
         "('Alisa', 17)",
         "('Burke', 21)",
         "('Aline', 3)",
         "('Alfreda', 38)",
         "('Hakeem', 48)",
         "('John', 15)",
         "('Kevin', 35)",
         "('Uta', 13)",
         "('Leslie', 38)",
         "('Remedios', 45)",
         "('Omar', 16)",
         "('Barclay', 34)",
         "('Melyssa', 32)",
         "('Noelle', 25)",
         "('Maxwell', 10)",
         "('Lucian', 47)",
         "('Zephania', 44)",
         "('Channing', 5)",
         "('Regan', 15)",
         "('Jelani', 9)",
         "('Simone', 1)",
         "('Galvin', 28)",
         "('Blossom', 9)",
         "('Joan', 36)",
         "('Forrest', 6)",
         "('Tamara', 4)",
         "('Rooney', 44)",
         "('Meghan', 15)",
         "('Robert', 3)",
         "('Imani', 25)",
         "('Ross', 16)",
         "('Cheyenne', 10)",
         "('Giacomo', 38)",
         "('Geraldine', 3)",
         "('Inga', 4)",
         "('Brenda', 39)",
         "('Lane', 12)",
         "('Maris', 18)",
         "('Adele', 50)",
         "('Ian', 33)",
         "('Phillip', 48)",
         "('Charity', 19)",
         "('Jael', 36)",
         "('Gabriel', 26)",
         "('Angelica', 38)",
         "('Marshall', 33)",
         "('Ashton', 31)",
         "('Dillon', 21)",
         "('Jolie', 15)",
         "('Madeline', 47)",
         "('Amethyst', 49)",
         "('Guinevere', 46)",
         "('Damian', 4)",
         "('Salvador', 31)",
         "('Charles', 33)",
         "('Gray', 13)",
         "('Tad', 24)",
         "('Ignatius', 26)",
         "('Simon', 49)",
         "('Raphael', 4)",
         "('Jillian', 10)",
         "('Kane', 36)",
         "('Jenette', 28)",
         "('Theodore', 45)",
         "('Julian', 27)",
         "('Derek', 24)",
         "('Hedy', 34)",
         "('Brock', 35)",
         "('Aiko', 4)",
         "('Bianca', 5)",                
         };

         for (int i=0; i<test5.length; i++)
            stmt.executeUpdate(s + test5[i]);
         System.out.println("Test 5 records inserted.");
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
