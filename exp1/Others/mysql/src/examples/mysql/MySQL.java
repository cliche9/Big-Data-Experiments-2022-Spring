package examples.mysql;

import java.io.BufferedReader;
import java.sql.*;

public class MySQLDemo {
	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String DB_URL = "jdbc:mysql://localhost:3306/exp1";
	private static final String USER = "hadoop";
	private static final String PASS = "sunqi2000";

	public static void main(String[] args) {
		Connection connection = null;
		Statement statement = null;

		try {
			// register jdbc driver
			Class.forName(JDBC_DRIVER);
			// open connection
			System.out.println("connecting...");
			connection = DriverManager.getConnection(DB_URL, USER, PASS);
			// query
			
		} catch (Exception e) {
			//TODO: handle exception
		}
	}
	
}
