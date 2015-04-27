//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.couchbase.connector.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.connection.StandardAttributes;
import com.informatica.cloud.api.adapter.plugin.InvalidArgumentException;

public class CBConnection implements IConnection {

	// Define a string as the fully qualified class name
	private static String jdbcDriver = "com.simba.couchbase.jdbc41.Driver";

	// Connection attributes, the key-values are passed from the Informatica
	// framework
	private Map<String, String> connAttribs = new HashMap<String, String>();

	// The JDBC connection object
	private Connection connection = null;

	@Override
	public boolean connect() throws InsufficientConnectInfoException,
			ConnectionFailedException {
		try {
			// Reuses an established connection if it is available.
			if (validate()) {
				return true;
			}
		} catch (Exception e) {
			throw new ConnectionFailedException(e);
		}

		if (connAttribs != null && !connAttribs.isEmpty()
				&& connAttribs.size() > 0) {
			String connectionURL = connAttribs
					.get(StandardAttributes.connectionUrl.getName());
			String userName = connAttribs.get(StandardAttributes.username
					.getName());
			String password = connAttribs.get(StandardAttributes.password
					.getName());

			// Check connection parameters
			if (connectionURL == null || userName == null || password == null) {
				throw new InsufficientConnectInfoException(
						"Missing connection parameters:" + connectionURL == null ? " (connection URL)"
								: "" + userName == null ? " (user name)" : ""
										+ password == null ? " (password)" : ""
										+ ".");
			}
			try {
				// Load the JDBC Driver class.
				Class.forName(jdbcDriver);

				if (connection != null) {
					connection.close();
				}

				// Establish a connection using the connection // URL
				connection = DriverManager.getConnection(connectionURL,
						userName, password);
			} catch (Exception e) {
				e.printStackTrace();
				throw new ConnectionFailedException(e);
			}

			// Reaching here without exceptions means the connection is
			// established.
			return true;
		} else {
			// The connection attribute map is missing or empty.
			return false;
		}
	}

	@Override
	public boolean disconnect() {
		if (connection == null) {
			return false;
		}
		try {
			connection.close();
			return true;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void setConnectionAttributes(Map<String, String> connParams) {
		this.connAttribs.clear();
		this.connAttribs.putAll(connParams);
	}

	@Override
	public boolean validate() throws InvalidArgumentException {
		if (connection == null) {
			return false;
		}
		try {
			return !connection.isClosed();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public Connection getConnection() {
		try {
			if (!validate()) {
				if (!connect()) {
					throw new ConnectionFailedException("connection failed");
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return connection;
	}

	@Override
	public IConnection clone() {
		IConnection newConnection = new CBConnection();
		newConnection.setConnectionAttributes(connAttribs);
		return newConnection;
	}

}
