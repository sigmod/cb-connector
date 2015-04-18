//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
		String[] types = { "TABLE" };
		DatabaseMetaData metadata = connection.getMetaData();
		ResultSet rs = metadata.getTables(null, null, "%", types);
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i < columnsNumber; i++) {
				System.out.println(rsmd.getColumnLabel(i) + ":"
						+ rs.getString(i) + " ");
				// if (rsmd.getColumnLabel(i).equals("TABLE_NAME")) {
				// if (rs.getString(i).contains("-")
				// || rs.getString(i).contains("default")) {
				// continue;
				// }
				// System.out.println(rs.getString(i));
				// ResultSet schema = metadata.getColumns(null, null,
				// "orders", null);
				// ResultSetMetaData schemaMetadata = rs.getMetaData();
				// for (int j = 1; j <= columnsNumber; j++) {
				// System.out.print(schemaMetadata.getColumnLabel(j) + ":"
				// + schema.getString(j) + " ");
				// }
				// System.out.println();
				// }
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
				System.out.print(rs.getString(i) + "|");
			}
			System.out.println();
			row++;
		}
		System.out.println(row);
		connection.close();
	}
}
