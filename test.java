/*
export CLASSPATH=.:postgresql-42.2.10.jar
javac test.java
java test
*/
import java.nio.charset.*;
import java.util.Random; 

import java.sql.*;
import java.util.*;

public class test {
    static void shuffleArray(Integer[] ar)
    {
        Random rnd = new Random();
        for (int i = ar.length - 1; i > 0; i--)
        {
            int index = rnd.nextInt(i + 1);
            int a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
  } 
    public static String randomString(int size){
        String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder builder = new StringBuilder();
        while (size-- != 0) {
            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

    public static class Generator{
        Connection conn;
        Integer[] keys = new Integer[5000*1000+1];
        public Generator(Connection conn){
            this.conn = conn;
            for(int i=0;i<=5000*1000;i++)
                keys[i] = i;
        }
        public void popData() throws SQLException{
            System.out.println("don't fuck w me");
        } 
    }

    public static class RandGenerator extends Generator{
        public RandGenerator(Connection conn){
            super(conn);
        }
        @Override
        public void popData() throws SQLException{
            shuffleArray(keys);
            Random rand = new Random(); 
            
            for(int j=0;j<100;j++){
                System.out.println("loading batch " + j + "out of 10");
                int start = j*5000*10;
                StringBuilder bder = new StringBuilder("insert into benchmark (theKey, columnA, columnB, filler)\n values\n");
	        	for(int i=0;i<5000*10;i++){
                   bder.append("(" + keys[start + i] + "," 
                        + ((Integer)(rand.nextInt(50000)+1)) + "," 
                        + ((Integer)(rand.nextInt(50000)+1)) + ","
                        + "'" + randomString(247) + "'" 
                        + "),\n");
                } 
		        bder.replace(bder.length()-2, bder.length(), ";\n");
                
                Statement stmt = conn.createStatement();
		        stmt.executeUpdate(bder.toString());
	            stmt.close();
            }
        }
    }

    public static class SeriGenerator extends Generator{
        public SeriGenerator(Connection conn){
            super(conn);
        }
        @Override
        public void popData() throws SQLException {
            Random rand = new Random(); 
            
            for(int j=0;j<10;j++){
                int start = j*5000*100;
                StringBuilder bder = new StringBuilder("insert into benchmark (theKey, columnA, columnB, filler)\n values\n");
	        	for(int i=0;i<5000*100;i++){
                   bder.append("(" + keys[start + i] + "," 
                        + ((Integer)(rand.nextInt(50000)+1)) + "," 
                        + ((Integer)(rand.nextInt(50000)+1)) + ","
                        + "'" + randomString(247) + "'" 
                        + "),\n");
                } 
		        bder.replace(bder.length()-2, bder.length(), ";\n");
                Statement stmt = conn.createStatement();
		        stmt.executeUpdate(bder.toString());
	            stmt.close();
            } 
        }
    }


	public static void createTable(Connection conn) throws SQLException {
		System.out.println("creating table");
	    String createString = 
	    	"create table benchmark ("+ 
	     	"theKey INT  PRIMARY KEY, " +
	     	"columnA INT, " +
            "columnB INT, " + 
            "filler CHAR(247)" + 
            ")" ;
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate(createString);
        stmt.close();
		System.out.println("table created!");
	}


	public static void insertRow(Connection conn) throws SQLException {
		// use batch insertion without autocommit to insert more rows at a time
		System.out.println("inserting row");
	    String insertString = 
	    	"insert into benchmark (theKey, columnA, columnB, filler)"+ 
	     	"values (423, 76453, 23453, 'shithole')";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(insertString);
	    stmt.close();
	}


	public static void printTable(Connection conn) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"select * from benchmark";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		while (rs.next()) {
	        
            System.out.println(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + ","  + rs.getString(4));
		}
		rs.close();
	    stmt.close();
	}

    public static void query(Connection conn, String query) throws SQLException{
  		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next()) {
            System.out.println(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + ","  + rs.getString(4));
		}
		rs.close();
	    stmt.close();
    }

    public static void q_index(Connection conn, String query) throws SQLException{
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }


	public static void dropTable(Connection conn) throws SQLException {
		System.out.println("dropping table");
	    String dropString = 
	    	"drop table benchmark";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(dropString);
	    stmt.close();
	}


	public static void main(String[] args) throws SQLException, ClassNotFoundException {

        System.out.println("loading driver");
		Class.forName("org.postgresql.Driver");
		System.out.println("driver loaded");

        System.out.println("Connecting to DB");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://database-1.cjcabtjymlhl.us-east-1.rds.amazonaws.com:5432/mydb?user=postgres&password=password"); password blurred

		// Connection conn = DriverManager.getConnection("jdbc:postgresql:postgres", "postgres", "secret");
		System.out.println("Connected to DB");
       
        long startTime;
        boolean createTable = true;
        if (createTable){
            try {
                // drops if there
                dropTable(conn);
            }
            catch (Exception e) {
                System.out.println("drop table exception :" + e.toString());
            }
            
            createTable(conn);
            
            Generator gen = new RandGenerator(conn);
            startTime = System.currentTimeMillis();
            gen.popData();
            long creatTime = System.currentTimeMillis() - startTime;
            System.out.println(creatTime);
        }

        // startTime = System.currentTimeMillis();
        // q_index(conn, "CREATE INDEX  a_index  ON benchmark(columnA)");
        // // q_index(conn, "CREATE INDEX  b_index  ON benchmark(columnB)");
        // // q_index(conn, "DROP INDEX  a_index");
        // long indexTime = System.currentTimeMillis() - startTime;
        // System.out.println(indexTime);

        String point1 = "25005";
        String point2 = "25005";
        System.out.println("********************** Query 1 *************************");
		startTime = System.currentTimeMillis();
        query(conn, "SELECT * FROM benchmark WHERE benchmark.columnA = " + point1);
        long t1 = System.currentTimeMillis() - startTime;
        
        // System.out.println("********************** Query 2 *************************");
        // startTime = System.currentTimeMillis();
        // query(conn, "SELECT * FROM benchmark WHERE benchmark.columnB = " + point2);
        // long t2 = System.currentTimeMillis() - startTime;

        // System.out.println("********************** Query 3 *************************");
        // startTime = System.currentTimeMillis();
        // query(conn, "SELECT * FROM benchmark WHERE benchmark.columnA = " + point1 + " AND benchmark.columnB = " + point2);
        // long t3= System.currentTimeMillis() - startTime;

 
        // System.out.println(t1);
        // System.out.println(t2);
        // System.out.println(t3);

        dropTable(conn);

	}

}
