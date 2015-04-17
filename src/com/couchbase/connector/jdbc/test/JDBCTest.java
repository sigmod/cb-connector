package com.couchbase.connector.jdbc.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.Test;

public class JDBCTest {

	private static String jdbcDriver = "com.simba.couchbase.jdbc41.Driver";
	private static String connectionURL = "jdbc:couchbase://localhost:8093;queryEnabled=1;logLevel=6;logPath=log";
	private static String username = "Administrator";
	private static String password = "password";

	@Test
	public void test() throws Exception {
		// Load the JDBC Driver class.
		Class.forName(jdbcDriver);
		// Establish a connection using the connection // URL
		Connection connection = DriverManager.getConnection(connectionURL,
				username, password);

		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet rs = metadata.getTables(null, null, "%", null);
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i < columnsNumber; i++) {
				System.out.println(rsmd.getColumnName(i) + ":"
						+ rs.getString(i) + " ");
			}
			System.out.println();
		}

		String query = "select * from product";
		Statement stmt = connection.createStatement();
		rs = stmt.executeQuery(query);
		rsmd = rs.getMetaData();
		columnsNumber = rsmd.getColumnCount();
		int row = 0;
		while (rs.next()) {
			for (int i = 1; i <= columnsNumber; i++) {
				System.out.print(rsmd.getColumnLabel(i) + ":" + rs.getString(i)
						+ " ");
			}
			System.out.println();
			row++;
		}
		System.out.println(row);
		connection.close();
	}
}
