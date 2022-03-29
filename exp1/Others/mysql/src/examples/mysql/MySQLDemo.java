package examples.mysql;

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
			if (!connection.isClosed())
				System.out.println("Successfully connected");
			statement = connection.createStatement();
			// check if existing
			if (!statement.executeQuery("select * from Student where Name = \"Scofield\";").next()) {
				// insert
				String insert = "insert into Student(Name, English, Math, Computer) values(\"Scofield\", 45, 89, 100);";
				statement.execute(insert);
				System.out.println("Successfully inserted");
			}
			String queryEnglish = "select English from Student where Name = \"Scofield\"";
			ResultSet resultSet = statement.executeQuery(queryEnglish);
			System.out.println("Successfully selected");
			System.out.println("English");
			while (resultSet.next())
				System.out.println(resultSet.getString("English"));
			resultSet.close();
			connection.close();
		} catch (ClassNotFoundException e) {
			System.out.println("Driver not found");
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
