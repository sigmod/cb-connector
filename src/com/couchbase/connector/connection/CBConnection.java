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
	private static String jdbcDriver = "com.simba.couchbase.jdbc4.Driver";

	// Connection attributes, the key-values are passed from the Informatica framework
	private Map<String, String> connAttribs = new HashMap<String, String>();
	
	// The JDBC connection object
	private Connection connection = null;

	@Override
	public boolean connect() throws InsufficientConnectInfoException,
			ConnectionFailedException {
		if (connAttribs != null && !connAttribs.isEmpty()
				&& connAttribs.size() > 0) {
			String connectionURL = connAttribs
					.get(StandardAttributes.connectionUrl);
			String userName = connAttribs.get(StandardAttributes.username);
			String password = connAttribs.get(StandardAttributes.password);

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

				// Establish a connection using the connection // URL
				connection = DriverManager.getConnection(connectionURL,
						userName, password);
			} catch (Exception e) {
				throw new ConnectionFailedException(e);
			}
		}
		// Reaching here without exceptions means the connection is established.
		return true;
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
		return connection;
	}

}
